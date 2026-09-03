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
package org.apache.kafka.storage.diskless;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

final class DisklessClassLoaderContext {

    private DisklessClassLoaderContext() {
    }

    static <T> T call(ClassLoader classLoader, Action<T> action) throws Exception {
        ClassLoader originalClassLoader = Thread.currentThread().getContextClassLoader();
        Thread.currentThread().setContextClassLoader(classLoader);
        try {
            return action.execute();
        } finally {
            Thread.currentThread().setContextClassLoader(originalClassLoader);
        }
    }

    /**
     * Wraps one isolated-runtime component so every call runs with the plugin context class loader
     * and the first {@code close()} releases the class-loader lease.
     *
     * <p>The lease is released even when the delegate fails to close: a component that cannot close
     * cleanly must not pin its runtime for the lifetime of the process. Subsequent {@code close()}
     * calls are no-ops.
     */
    static <T> T leased(Class<T> type, T delegate, DisklessClassLoaderRegistry.Lease lease) {
        Objects.requireNonNull(delegate, "delegate must not be null");
        Objects.requireNonNull(lease, "lease must not be null");
        return type.cast(Proxy.newProxyInstance(
                type.getClassLoader(),
                new Class<?>[] {type},
                new LeasedHandler(delegate, lease)));
    }

    private static Object invoke(Method method, Object target, Object[] args) throws Exception {
        try {
            return method.invoke(target, args);
        } catch (InvocationTargetException e) {
            Throwable cause = e.getCause();
            if (cause instanceof Exception exception) {
                throw exception;
            }
            if (cause instanceof Error error) {
                throw error;
            }
            throw e;
        }
    }

    /**
     * A named handler rather than a lambda: it keeps the isolated delegate discoverable in stack
     * traces and from tests that reflect into the plugin runtime.
     */
    private static final class LeasedHandler implements InvocationHandler {
        private final Object delegate;
        private final DisklessClassLoaderRegistry.Lease lease;
        private final AtomicBoolean closed = new AtomicBoolean();

        private LeasedHandler(Object delegate, DisklessClassLoaderRegistry.Lease lease) {
            this.delegate = delegate;
            this.lease = lease;
        }

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            if (method.getDeclaringClass() == Object.class) {
                // The proxy, not the delegate, is the component every caller holds, so it keeps its
                // own identity: forwarding equals/hashCode would make proxy.equals(proxy) false.
                // toString still forwards, so logs and stack traces name the isolated delegate.
                if ("equals".equals(method.getName()) && method.getParameterCount() == 1) {
                    return proxy == args[0];
                }
                if ("hashCode".equals(method.getName()) && method.getParameterCount() == 0) {
                    return System.identityHashCode(proxy);
                }
                return method.invoke(delegate, args);
            }
            if ("close".equals(method.getName()) && method.getParameterCount() == 0) {
                if (!closed.compareAndSet(false, true)) {
                    return null;
                }
                try {
                    return call(lease.classLoader(), () -> DisklessClassLoaderContext.invoke(method, delegate, args));
                } finally {
                    lease.close();
                }
            }
            return call(lease.classLoader(), () -> DisklessClassLoaderContext.invoke(method, delegate, args));
        }
    }

    interface Action<T> {
        T execute() throws Exception;
    }
}
