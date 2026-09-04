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
package org.apache.kafka.server.ursa.integration;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.test.KafkaClusterTestKit;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * Test-only inspection of the Lakestream catalog loaded inside the broker's isolated Ursa runtime.
 *
 * <p>Lakestream API classes are plugin-private and must not be linked from the server test class
 * loader. This inspector therefore reflects through the class loader that already owns the broker's
 * Ursa engine and exposes only JDK values to its callers.
 */
final class IsolatedUrsaCatalogInspector implements AutoCloseable {
    private static final String KAFKA_STREAM_IDENTITY_CLASS =
            "org.apache.kafka.storage.diskless.handlers.KafkaStreamIdentity";
    private static final String PRODUCER_STATE_SNAPSHOT_KEYS_CLASS =
            "org.apache.kafka.storage.diskless.idempotent.ProducerStateSnapshotKeys";
    private static final String STREAM_CATALOG_CLASS = "io.lakestream.api.StreamCatalog";
    private static final String STREAM_CATALOG_LOADER_CLASS = "io.lakestream.api.StreamCatalogLoader";
    private static final String STREAM_IDENTIFIER_CLASS = "io.lakestream.api.StreamIdentifier";
    private static final String STREAM_METADATA_CLASS = "io.lakestream.api.StreamMetadata";
    private static final String PARTITIONING_CLASS = "io.lakestream.api.Partitioning";
    private static final String STREAM_LAYOUT_CLASS = "io.lakestream.api.StreamLayout";
    private static final String LOG_ID_CLASS = "io.lakestream.api.LogId";
    private static final String LOG_CLASS = "io.lakestream.api.Log";
    private static final String LOG_OFFSET_CLASS = "io.lakestream.api.LogOffset";
    private static final String ENTRY_INDEX_CLASS = "io.lakestream.api.EntryIndex";
    private static final String POSITION_CLASS = "io.lakestream.api.Position";

    private final ClassLoader classLoader;
    private final Object catalog;
    private final Method identifierOfMethod;
    private final Method listStreamsMethod;
    private final Method streamExistsMethod;
    private final Method loadStreamMethod;
    private final Method metadataPartitioningMethod;
    private final Method partitionCountMethod;
    private final Method metadataLayoutMethod;
    private final Method layoutLogIdsMethod;
    private final Method openLogMethod;
    private final Method logIdValueMethod;
    private final Method logLastOffsetMethod;
    private final Method logReadIndexRangeMethod;
    private final Method logOffsetValueMethod;
    private final Method logOffsetRecordCountMethod;
    private final Method entryIndexPositionMethod;
    private final Method positionFileTypeMethod;
    private boolean closed;

    static IsolatedUrsaCatalogInspector open(
            KafkaClusterTestKit cluster,
            String catalogUri,
            Properties properties
    ) throws Exception {
        Object disklessStorageSupport = cluster.brokers().values().iterator().next()
                .replicaManager().disklessStorageSupport();
        Object engine = disklessStorageEngine(disklessStorageSupport);
        return new IsolatedUrsaCatalogInspector(engine, catalogUri, properties);
    }

    static Object disklessStorageEngine(Object disklessStorageSupport) throws Exception {
        Field engineField = findField(disklessStorageSupport.getClass(), "engine");
        if (engineField == null) {
            throw new IllegalStateException("Diskless storage support does not expose its engine field");
        }
        engineField.setAccessible(true);
        Object engine = engineField.get(disklessStorageSupport);
        if (engine == null) {
            throw new IllegalStateException("Diskless storage engine has not been initialized");
        }
        return engine;
    }

    static String namespace(KafkaClusterTestKit cluster) throws Exception {
        return implementationStringConstant(cluster, KAFKA_STREAM_IDENTITY_CLASS, "NAMESPACE");
    }

    static String kafkaManagedProperty(KafkaClusterTestKit cluster) throws Exception {
        return implementationStringConstant(
                cluster, KAFKA_STREAM_IDENTITY_CLASS, "KAFKA_MANAGED_PROPERTY");
    }

    static String sourceLogicalNameProperty(KafkaClusterTestKit cluster) throws Exception {
        return implementationStringConstant(
                cluster, KAFKA_STREAM_IDENTITY_CLASS, "SOURCE_LOGICAL_NAME_PROPERTY");
    }

    static String kafkaTopicNameProperty(KafkaClusterTestKit cluster) throws Exception {
        return implementationStringConstant(
                cluster, KAFKA_STREAM_IDENTITY_CLASS, "KAFKA_TOPIC_NAME_PROPERTY");
    }

    static String kafkaTopicIdProperty(KafkaClusterTestKit cluster) throws Exception {
        return implementationStringConstant(
                cluster, KAFKA_STREAM_IDENTITY_CLASS, "KAFKA_TOPIC_ID_PROPERTY");
    }

    static String kafkaSourceRevisionProperty(KafkaClusterTestKit cluster) throws Exception {
        return implementationStringConstant(
                cluster, KAFKA_STREAM_IDENTITY_CLASS, "KAFKA_SOURCE_REVISION_PROPERTY");
    }

    static String streamName(KafkaClusterTestKit cluster, String topicName, Uuid topicId) throws Exception {
        return implementationStatic(
                cluster,
                KAFKA_STREAM_IDENTITY_CLASS,
                "streamName",
                new Class<?>[]{String.class, Uuid.class},
                topicName,
                topicId);
    }

    static String producerStateTopicIndexName(KafkaClusterTestKit cluster) throws Exception {
        return implementationStatic(
                cluster,
                PRODUCER_STATE_SNAPSHOT_KEYS_CLASS,
                "topicIndexName",
                new Class<?>[0]);
    }

    static String producerStateTopicIndexKey(KafkaClusterTestKit cluster, String topicId) throws Exception {
        return implementationStatic(
                cluster,
                PRODUCER_STATE_SNAPSHOT_KEYS_CLASS,
                "topicIndexKey",
                new Class<?>[]{String.class},
                topicId);
    }

    static String producerStateSnapshotKey(
            KafkaClusterTestKit cluster,
            String topicId,
            int partition
    ) throws Exception {
        return implementationStatic(
                cluster,
                PRODUCER_STATE_SNAPSHOT_KEYS_CLASS,
                "snapshotKey",
                new Class<?>[]{String.class, int.class},
                topicId,
                partition);
    }

    static String producerStateSnapshotKey(
            KafkaClusterTestKit cluster,
            String topicId,
            int partition,
            String zone
    ) throws Exception {
        return implementationStatic(
                cluster,
                PRODUCER_STATE_SNAPSHOT_KEYS_CLASS,
                "snapshotKey",
                new Class<?>[]{String.class, int.class, String.class},
                topicId,
                partition,
                zone);
    }

    private IsolatedUrsaCatalogInspector(
            Object ursaEngine,
            String catalogUri,
            Properties properties
    ) throws Exception {
        classLoader = isolatedClassLoader(ursaEngine);

        Class<?> catalogLoaderClass = loadClass(STREAM_CATALOG_LOADER_CLASS);
        if (catalogLoaderClass.getClassLoader() != classLoader) {
            throw new IllegalStateException("Lakestream API was loaded outside the isolated Ursa class loader");
        }

        Class<?> identifierClass = loadClass(STREAM_IDENTIFIER_CLASS);
        Class<?> catalogClass = loadClass(STREAM_CATALOG_CLASS);
        Class<?> metadataClass = loadClass(STREAM_METADATA_CLASS);
        Class<?> partitioningClass = loadClass(PARTITIONING_CLASS);
        Class<?> layoutClass = loadClass(STREAM_LAYOUT_CLASS);
        Class<?> logIdClass = loadClass(LOG_ID_CLASS);
        Class<?> logClass = loadClass(LOG_CLASS);
        Class<?> logOffsetClass = loadClass(LOG_OFFSET_CLASS);
        Class<?> entryIndexClass = loadClass(ENTRY_INDEX_CLASS);
        Class<?> positionClass = loadClass(POSITION_CLASS);

        identifierOfMethod = identifierClass.getMethod("of", String.class, String.class);
        listStreamsMethod = catalogClass.getMethod("listStreams", String.class);
        streamExistsMethod = catalogClass.getMethod("streamExists", identifierClass);
        loadStreamMethod = catalogClass.getMethod("loadStream", identifierClass);
        metadataPartitioningMethod = metadataClass.getMethod("partitioning");
        partitionCountMethod = partitioningClass.getMethod("numPartitions");
        metadataLayoutMethod = metadataClass.getMethod("layout");
        layoutLogIdsMethod = layoutClass.getMethod("logIds");
        openLogMethod = catalogClass.getMethod("openLog", identifierClass, logIdClass);
        logIdValueMethod = logIdClass.getMethod("id");
        logLastOffsetMethod = logClass.getMethod("getLastOffset");
        logReadIndexRangeMethod = logClass.getMethod("readIndexRange", long.class, long.class);
        logOffsetValueMethod = logOffsetClass.getMethod("offset");
        logOffsetRecordCountMethod = logOffsetClass.getMethod("numberOfRecords");
        entryIndexPositionMethod = entryIndexClass.getMethod("position");
        positionFileTypeMethod = positionClass.getMethod("fileType");

        Method openMethod = catalogLoaderClass.getMethod(
                "open", String.class, Properties.class, ClassLoader.class);
        catalog = withContextClassLoader(() -> invoke(
                openMethod, null, catalogUri, properties, classLoader));
    }

    boolean isStreamListed(
            String namespace,
            String streamName,
            long timeout,
            TimeUnit unit
    ) throws Exception {
        return withContextClassLoader(() -> {
            Object identifier = identifier(namespace, streamName);
            List<?> streams = result(listStreamsMethod, catalog, timeout, unit, namespace);
            return streams.contains(identifier);
        });
    }

    boolean streamExists(
            String namespace,
            String streamName,
            long timeout,
            TimeUnit unit
    ) throws Exception {
        return withContextClassLoader(() -> {
            Object identifier = identifier(namespace, streamName);
            return result(streamExistsMethod, catalog, timeout, unit, identifier);
        });
    }

    int partitionCount(
            String namespace,
            String streamName,
            long timeout,
            TimeUnit unit
    ) throws Exception {
        return withContextClassLoader(() -> {
            Object metadata = loadStream(namespace, streamName, timeout, unit);
            Object partitioning = invoke(metadataPartitioningMethod, metadata);
            return ((Number) invoke(partitionCountMethod, partitioning)).intValue();
        });
    }

    List<Long> partitionLogIds(
            String namespace,
            String streamName,
            long timeout,
            TimeUnit unit
    ) throws Exception {
        return withContextClassLoader(() -> {
            Object metadata = loadStream(namespace, streamName, timeout, unit);
            Object layout = invoke(metadataLayoutMethod, metadata);
            List<?> logIds = result(layoutLogIdsMethod, layout, timeout, unit);
            List<Long> values = new ArrayList<>(logIds.size());
            for (Object logId : logIds) {
                values.add(((Number) invoke(logIdValueMethod, logId)).longValue());
            }
            return List.copyOf(values);
        });
    }

    LogIndexSummary readLogIndexSummary(
            String namespace,
            String streamName,
            int partition,
            long timeout,
            TimeUnit unit
    ) throws Exception {
        return withContextClassLoader(() -> {
            Object identifier = identifier(namespace, streamName);
            Object metadata = result(loadStreamMethod, catalog, timeout, unit, identifier);
            Object log = null;
            try {
                Object layout = invoke(metadataLayoutMethod, metadata);
                List<?> logIds = result(layoutLogIdsMethod, layout, timeout, unit);
                Object logId = logIds.get(partition);
                long logIdValue = ((Number) invoke(logIdValueMethod, logId)).longValue();
                log = result(openLogMethod, catalog, timeout, unit, identifier, logId);

                Object lastOffset = result(logLastOffsetMethod, log, timeout, unit);
                long offset = ((Number) invoke(logOffsetValueMethod, lastOffset)).longValue();
                if (offset < 0) {
                    return new LogIndexSummary(logIdValue, offset, 0, 0);
                }

                int numberOfRecords = ((Number) invoke(logOffsetRecordCountMethod, lastOffset)).intValue();
                List<?> indexes = result(
                        logReadIndexRangeMethod,
                        log,
                        timeout,
                        unit,
                        0L,
                        offset + numberOfRecords);
                int parquetIndexCount = 0;
                for (Object index : indexes) {
                    Object position = invoke(entryIndexPositionMethod, index);
                    Object fileType = invoke(positionFileTypeMethod, position);
                    if (fileType instanceof Enum<?> enumValue && "PARQUET".equals(enumValue.name())) {
                        parquetIndexCount++;
                    }
                }
                return new LogIndexSummary(logIdValue, offset, indexes.size(), parquetIndexCount);
            } finally {
                if (log != null) {
                    closeResource(log);
                }
            }
        });
    }

    @Override
    public void close() throws Exception {
        if (!closed) {
            withContextClassLoader(() -> {
                closeResource(catalog);
                return null;
            });
            closed = true;
        }
    }

    private Object loadStream(
            String namespace,
            String streamName,
            long timeout,
            TimeUnit unit
    ) throws Exception {
        return result(loadStreamMethod, catalog, timeout, unit, identifier(namespace, streamName));
    }

    private Object identifier(String namespace, String streamName) throws Exception {
        return invoke(identifierOfMethod, null, namespace, streamName);
    }

    private Class<?> loadClass(String className) throws ClassNotFoundException {
        return Class.forName(className, true, classLoader);
    }

    @SuppressWarnings("unchecked")
    private <T> T result(
            Method method,
            Object target,
            long timeout,
            TimeUnit unit,
            Object... arguments
    ) throws Exception {
        CompletableFuture<?> future = (CompletableFuture<?>) invoke(method, target, arguments);
        return (T) future.get(timeout, unit);
    }

    private static Object invoke(Method method, Object target, Object... arguments) throws Exception {
        try {
            return method.invoke(target, arguments);
        } catch (InvocationTargetException e) {
            Throwable cause = e.getCause();
            if (cause instanceof Exception exception) {
                throw exception;
            }
            if (cause instanceof Error error) {
                throw error;
            }
            throw new RuntimeException(cause);
        }
    }

    private static String implementationStringConstant(
            KafkaClusterTestKit cluster,
            String className,
            String fieldName
    ) throws Exception {
        ClassLoader implementationClassLoader = implementationClassLoader(cluster);
        return withContextClassLoader(implementationClassLoader, () -> {
            Class<?> implementationClass = Class.forName(className, true, implementationClassLoader);
            return (String) implementationClass.getField(fieldName).get(null);
        });
    }

    @SuppressWarnings("unchecked")
    private static <T> T implementationStatic(
            KafkaClusterTestKit cluster,
            String className,
            String methodName,
            Class<?>[] parameterTypes,
            Object... arguments
    ) throws Exception {
        ClassLoader implementationClassLoader = implementationClassLoader(cluster);
        return withContextClassLoader(implementationClassLoader, () -> {
            Class<?> implementationClass = Class.forName(className, true, implementationClassLoader);
            Method method = implementationClass.getMethod(methodName, parameterTypes);
            return (T) invoke(method, null, arguments);
        });
    }

    private static ClassLoader implementationClassLoader(KafkaClusterTestKit cluster) throws Exception {
        Object disklessStorageSupport = cluster.brokers().values().iterator().next()
                .replicaManager().disklessStorageSupport();
        return isolatedClassLoader(disklessStorageEngine(disklessStorageSupport));
    }

    private static void closeResource(Object resource) throws Exception {
        ((AutoCloseable) resource).close();
    }

    private <T> T withContextClassLoader(CheckedSupplier<T> action) throws Exception {
        return withContextClassLoader(classLoader, action);
    }

    private static <T> T withContextClassLoader(
            ClassLoader contextClassLoader,
            CheckedSupplier<T> action
    ) throws Exception {
        Thread thread = Thread.currentThread();
        ClassLoader originalClassLoader = thread.getContextClassLoader();
        thread.setContextClassLoader(contextClassLoader);
        try {
            return action.get();
        } finally {
            thread.setContextClassLoader(originalClassLoader);
        }
    }

    private static ClassLoader isolatedClassLoader(Object ursaEngine) throws Exception {
        Object leaseHolder = leasedDelegateHolder(ursaEngine);
        Object pluginEngine = ursaEngine;
        Field delegateField = findField(leaseHolder.getClass(), "delegate");
        if (delegateField != null) {
            delegateField.setAccessible(true);
            pluginEngine = delegateField.get(leaseHolder);
        }

        ClassLoader candidate = pluginEngine.getClass().getClassLoader();
        if (candidate == null || candidate == IsolatedUrsaCatalogInspector.class.getClassLoader()) {
            throw new IllegalStateException("Ursa engine is not loaded by an isolated class loader");
        }
        return candidate;
    }

    /**
     * Returns the object holding the isolated delegate of a leased diskless component: the
     * component itself when it is not proxied, and the proxy's invocation handler otherwise.
     */
    static Object leasedDelegateHolder(Object component) {
        return Proxy.isProxyClass(component.getClass())
                ? Proxy.getInvocationHandler(component)
                : component;
    }

    private static Field findField(Class<?> type, String name) {
        for (Class<?> current = type; current != null; current = current.getSuperclass()) {
            try {
                return current.getDeclaredField(name);
            } catch (NoSuchFieldException ignored) {
                // Continue with the superclass.
            }
        }
        return null;
    }

    record LogIndexSummary(long logId, long lastOffset, int indexCount, int parquetIndexCount) {
    }

    @FunctionalInterface
    private interface CheckedSupplier<T> {
        T get() throws Exception;
    }
}
