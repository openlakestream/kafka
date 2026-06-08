/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kafka.server;

import kafka.cluster.Partition;

import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.utils.KafkaPluginClassLoader;
import org.apache.kafka.metadata.ConfigRepository;
import org.apache.kafka.storage.internals.log.LogAppendInfo;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ClassLoaderAwareReplicaManagerInterceptorTest {

    @TempDir
    Path tempDir;

    @Test
    void testInterceptorInvocationsUsePluginClassLoader() throws Exception {
        CloseTrackingClassLoader classLoader = new CloseTrackingClassLoader();
        CapturingInterceptor delegate = new CapturingInterceptor();
        ClassLoaderAwareReplicaManagerInterceptor interceptor =
                new ClassLoaderAwareReplicaManagerInterceptor(delegate, classLoader);

        interceptor.onAppend(null, null, null);
        assertSame(classLoader, delegate.onAppendClassLoader.get());

        interceptor.close();
        assertSame(classLoader, delegate.closeClassLoader.get());
        assertTrue(classLoader.closed.get());
    }

    @Test
    void testClosePreservesDelegateErrorWhenClassLoaderCloseFails() {
        FailingCloseClassLoader classLoader = new FailingCloseClassLoader();
        ClassLoaderAwareReplicaManagerInterceptor interceptor =
                new ClassLoaderAwareReplicaManagerInterceptor(new ErrorOnCloseInterceptor(), classLoader);

        NoClassDefFoundError error = assertThrows(NoClassDefFoundError.class, interceptor::close);

        assertTrue(classLoader.closed.get());
        assertEquals(1, error.getSuppressed().length);
        assertTrue(error.getSuppressed()[0] instanceof IOException);
    }

    @Test
    void testNewInstanceLoadsInterceptorFromPluginClassLoader() throws Exception {
        Path childDir = Files.createDirectory(tempDir.resolve("child"));
        copyClassToDirectory(ConstructedInterceptor.class, childDir);

        ClassLoader parent = new HideConstructedInterceptorClassLoader();
        try (KafkaPluginClassLoader classLoader = new KafkaPluginClassLoader(childDir.toString(), parent)) {
            ReplicaManagerInterceptor interceptor = ClassLoaderAwareReplicaManagerInterceptor.newInstance(
                    ConstructedInterceptor.class.getName(),
                    null,
                    null,
                    classLoader);

            interceptor.onAppend(null, null, null);
            interceptor.close();
        }
    }

    @Test
    void testDefaultClassPathUsesKafkaHomeUrsaStorageDirectory() throws Exception {
        Path kafkaHome = Files.createDirectory(tempDir.resolve("kafka-home"));
        String originalKafkaHome = System.getProperty("kafka.home");
        System.setProperty("kafka.home", kafkaHome.toString());
        try {
            assertSamePath(
                    new File(kafkaHome.toFile(), "ursa-storage" + File.separator + "*").getPath(),
                    ClassLoaderAwareReplicaManagerInterceptor.effectiveClassPath(""));
        } finally {
            restoreKafkaHome(originalKafkaHome);
        }
    }

    @Test
    void testNewInstanceUsesPluginContextWhenClassPathIsEmpty() throws Exception {
        ConstructedInterceptor.reset();
        ClassLoader parent = ClassLoaderAwareReplicaManagerInterceptorTest.class.getClassLoader();
        ReplicaManagerInterceptor interceptor = ClassLoaderAwareReplicaManagerInterceptor.newInstance(
                ConstructedInterceptor.class.getName(),
                null,
                null,
                "");

        interceptor.onAppend(null, null, null);
        interceptor.close();

        assertTrue(ConstructedInterceptor.CONSTRUCTOR_CLASS_LOADER.get() instanceof KafkaPluginClassLoader);
        assertTrue(ConstructedInterceptor.ON_APPEND_CLASS_LOADER.get() instanceof KafkaPluginClassLoader);
        assertTrue(ConstructedInterceptor.CLOSE_CLASS_LOADER.get() instanceof KafkaPluginClassLoader);
        assertNotSame(parent, ConstructedInterceptor.CONSTRUCTOR_CLASS_LOADER.get());
    }

    private void copyClassToDirectory(Class<?> clazz, Path directory) throws Exception {
        String resourceName = clazz.getName().replace('.', '/') + ".class";
        Path classFile = directory.resolve(resourceName);
        Files.createDirectories(classFile.getParent());
        try (var input = clazz.getClassLoader().getResourceAsStream(resourceName)) {
            assertNotNull(input);
            Files.copy(input, classFile, StandardCopyOption.REPLACE_EXISTING);
        }
    }

    private void assertSamePath(String expected, String actual) throws Exception {
        assertEquals(
                new File(expected).getCanonicalFile(),
                new File(actual).getCanonicalFile());
    }

    private void restoreKafkaHome(String originalKafkaHome) {
        if (originalKafkaHome == null) {
            System.clearProperty("kafka.home");
        } else {
            System.setProperty("kafka.home", originalKafkaHome);
        }
    }

    private static final class CapturingInterceptor implements ReplicaManagerInterceptor {
        private final AtomicReference<ClassLoader> onAppendClassLoader = new AtomicReference<>();
        private final AtomicReference<ClassLoader> closeClassLoader = new AtomicReference<>();

        @Override
        public void onAppend(MemoryRecords records, LogAppendInfo appendInfo, Partition partition) {
            onAppendClassLoader.set(Thread.currentThread().getContextClassLoader());
        }

        @Override
        public void close() {
            closeClassLoader.set(Thread.currentThread().getContextClassLoader());
        }
    }

    private static final class CloseTrackingClassLoader extends ClassLoader implements Closeable {
        private final AtomicBoolean closed = new AtomicBoolean(false);

        @Override
        public void close() throws IOException {
            closed.set(true);
        }
    }

    private static final class FailingCloseClassLoader extends ClassLoader implements Closeable {
        private final AtomicBoolean closed = new AtomicBoolean(false);

        @Override
        public void close() throws IOException {
            closed.set(true);
            throw new IOException("close failed");
        }
    }

    private static final class ErrorOnCloseInterceptor implements ReplicaManagerInterceptor {

        @Override
        public void onAppend(MemoryRecords records, LogAppendInfo appendInfo, Partition partition) {
        }

        @Override
        public void close() {
            throw new NoClassDefFoundError("plugin class");
        }
    }

    public static final class ConstructedInterceptor implements ReplicaManagerInterceptor {
        static final AtomicReference<ClassLoader> CONSTRUCTOR_CLASS_LOADER = new AtomicReference<>();
        static final AtomicReference<ClassLoader> ON_APPEND_CLASS_LOADER = new AtomicReference<>();
        static final AtomicReference<ClassLoader> CLOSE_CLASS_LOADER = new AtomicReference<>();

        static void reset() {
            CONSTRUCTOR_CLASS_LOADER.set(null);
            ON_APPEND_CLASS_LOADER.set(null);
            CLOSE_CLASS_LOADER.set(null);
        }

        public ConstructedInterceptor(KafkaConfig clusterConfig, ConfigRepository topicConfigRepository) {
            CONSTRUCTOR_CLASS_LOADER.set(Thread.currentThread().getContextClassLoader());
        }

        @Override
        public void onAppend(MemoryRecords records, LogAppendInfo appendInfo, Partition partition) {
            ON_APPEND_CLASS_LOADER.set(Thread.currentThread().getContextClassLoader());
        }

        @Override
        public void close() {
            CLOSE_CLASS_LOADER.set(Thread.currentThread().getContextClassLoader());
        }
    }

    private static final class HideConstructedInterceptorClassLoader extends ClassLoader {
        private HideConstructedInterceptorClassLoader() {
            super(ClassLoaderAwareReplicaManagerInterceptorTest.class.getClassLoader());
        }

        @Override
        protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
            if (ConstructedInterceptor.class.getName().equals(name)) {
                throw new ClassNotFoundException(name);
            }
            return super.loadClass(name, resolve);
        }
    }
}
