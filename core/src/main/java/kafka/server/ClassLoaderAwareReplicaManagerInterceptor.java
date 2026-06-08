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

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.utils.KafkaPluginClassLoader;
import org.apache.kafka.metadata.ConfigRepository;
import org.apache.kafka.server.util.KafkaPluginClassPaths;
import org.apache.kafka.storage.internals.log.LogAppendInfo;

import com.google.common.annotations.VisibleForTesting;

import java.io.Closeable;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

/**
 * A wrapper for replica manager interceptors loaded from an isolated class loader.
 */
public class ClassLoaderAwareReplicaManagerInterceptor implements ReplicaManagerInterceptor {
    private static final String DEFAULT_URSA_STORAGE_DIR = "ursa-storage";

    private final ReplicaManagerInterceptor delegate;
    private final ClassLoader classLoader;

    @VisibleForTesting
    protected ClassLoaderAwareReplicaManagerInterceptor(
            ReplicaManagerInterceptor delegate,
            ClassLoader classLoader) {
        this.delegate = delegate;
        this.classLoader = classLoader;
    }

    public static ReplicaManagerInterceptor newInstance(
            String interceptorClassName,
            KafkaConfig clusterConfig,
            ConfigRepository topicConfigRepository,
            String classPath) {
        ClassLoader parentClassLoader = ClassLoaderAwareReplicaManagerInterceptor.class.getClassLoader();
        ClassLoader classLoader = new KafkaPluginClassLoader(effectiveClassPath(classPath), parentClassLoader);
        return newInstance(interceptorClassName, clusterConfig, topicConfigRepository, classLoader);
    }

    static ReplicaManagerInterceptor newInstance(
            String interceptorClassName,
            KafkaConfig clusterConfig,
            ConfigRepository topicConfigRepository,
            KafkaPluginClassLoader classLoader) {
        return newInstance(interceptorClassName, clusterConfig, topicConfigRepository, (ClassLoader) classLoader);
    }

    private static ReplicaManagerInterceptor newInstance(
            String interceptorClassName,
            KafkaConfig clusterConfig,
            ConfigRepository topicConfigRepository,
            ClassLoader classLoader) {
        try {
            ReplicaManagerInterceptor delegate = withClassLoader(classLoader, () -> {
                Class<?> interceptorClass = Class.forName(interceptorClassName, true, classLoader);
                Constructor<?> constructor = interceptorClass.getConstructor(KafkaConfig.class, ConfigRepository.class);
                return (ReplicaManagerInterceptor) constructor.newInstance(clusterConfig, topicConfigRepository);
            });
            return new ClassLoaderAwareReplicaManagerInterceptor(delegate, classLoader);
        } catch (Throwable e) {
            closeClassLoader(classLoader, e);
            throw new KafkaException("Failed to load replica manager interceptor " + interceptorClassName, e);
        }
    }

    @Override
    public void onAppend(MemoryRecords records, LogAppendInfo appendInfo, Partition partition) {
        try {
            withClassLoader(classLoader, () -> {
                delegate.onAppend(records, appendInfo, partition);
                return null;
            });
        } catch (RuntimeException | Error e) {
            throw e;
        } catch (Exception e) {
            throw new KafkaException("Failed to invoke replica manager interceptor", e);
        }
    }

    @Override
    public void close() throws Exception {
        Throwable thrown = null;
        try {
            withClassLoader(classLoader, () -> {
                delegate.close();
                return null;
            });
        } catch (Exception | Error t) {
            thrown = t;
            throw t;
        } finally {
            closeClassLoader(classLoader, thrown);
        }
    }

    private static void closeClassLoader(ClassLoader classLoader, Throwable cause) {
        if (classLoader instanceof Closeable closeable) {
            try {
                closeable.close();
            } catch (Exception e) {
                if (cause != null) {
                    cause.addSuppressed(e);
                } else {
                    throw new KafkaException("Failed to close replica manager interceptor class loader", e);
                }
            }
        }
    }

    private static <T> T withClassLoader(ClassLoader classLoader, ClassLoaderAction<T> action) throws Exception {
        ClassLoader originalClassLoader = Thread.currentThread().getContextClassLoader();
        Thread.currentThread().setContextClassLoader(classLoader);
        try {
            return action.execute();
        } catch (InvocationTargetException e) {
            Throwable cause = e.getCause();
            if (cause instanceof Exception exception) {
                throw exception;
            }
            if (cause instanceof Error error) {
                throw error;
            }
            throw e;
        } finally {
            Thread.currentThread().setContextClassLoader(originalClassLoader);
        }
    }

    @VisibleForTesting
    static String effectiveClassPath(String configuredClassPath) {
        return KafkaPluginClassPaths.configuredOrDefault(
                configuredClassPath,
                DEFAULT_URSA_STORAGE_DIR,
                ClassLoaderAwareReplicaManagerInterceptor.class);
    }

    private interface ClassLoaderAction<T> {
        T execute() throws Exception;
    }
}
