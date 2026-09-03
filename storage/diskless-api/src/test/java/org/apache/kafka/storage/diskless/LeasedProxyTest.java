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

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class LeasedProxyTest {

    interface Component extends AutoCloseable {
        String ping();
    }

    @Test
    void proxyKeepsItsOwnIdentityAndForwardsToString() throws Exception {
        DisklessClassLoaderRegistry.Lease lease = mock(DisklessClassLoaderRegistry.Lease.class);
        when(lease.classLoader()).thenReturn(getClass().getClassLoader());
        Component delegate = new Component() {
            @Override
            public String ping() {
                return "pong";
            }

            @Override
            public void close() {
            }

            @Override
            public String toString() {
                return "the-delegate";
            }
        };
        Component leased = DisklessClassLoaderContext.leased(Component.class, delegate, lease);

        // The proxy, not the delegate, is what callers hold and put in collections.
        assertEquals(leased, leased);
        assertNotEquals(leased, delegate);
        assertEquals(System.identityHashCode(leased), leased.hashCode());
        assertEquals("the-delegate", leased.toString());
    }

    @Test
    void closeReleasesLeaseEvenWhenDelegateCloseThrows() throws Exception {
        DisklessClassLoaderRegistry.Lease lease = mock(DisklessClassLoaderRegistry.Lease.class);
        when(lease.classLoader()).thenReturn(getClass().getClassLoader());
        Component delegate = mock(Component.class);
        doThrow(new IOException("close failed")).when(delegate).close();
        Component leased = DisklessClassLoaderContext.leased(Component.class, delegate, lease);

        assertThrows(IOException.class, leased::close);
        verify(lease).close();
        leased.close();
        verify(delegate, times(1)).close();
    }

    @Test
    void callsRunWithPluginClassLoader() throws Exception {
        try (URLClassLoader plugin = new URLClassLoader(new URL[0], getClass().getClassLoader())) {
            DisklessClassLoaderRegistry.Lease lease = mock(DisklessClassLoaderRegistry.Lease.class);
            when(lease.classLoader()).thenReturn(plugin);
            Component leased = DisklessClassLoaderContext.leased(Component.class, new Component() {
                @Override
                public String ping() {
                    return Thread.currentThread().getContextClassLoader() == plugin ? "plugin" : "other";
                }

                @Override
                public void close() {
                }
            }, lease);

            assertEquals("plugin", leased.ping());
        }
    }

    @Test
    void restoresTheCallerClassLoaderAfterEachCall() throws Exception {
        try (URLClassLoader plugin = new URLClassLoader(new URL[0], getClass().getClassLoader())) {
            DisklessClassLoaderRegistry.Lease lease = mock(DisklessClassLoaderRegistry.Lease.class);
            when(lease.classLoader()).thenReturn(plugin);
            Component delegate = mock(Component.class);
            when(delegate.ping()).thenReturn("pong");
            Component leased = DisklessClassLoaderContext.leased(Component.class, delegate, lease);

            ClassLoader callerClassLoader = Thread.currentThread().getContextClassLoader();
            assertEquals("pong", leased.ping());
            assertEquals(callerClassLoader, Thread.currentThread().getContextClassLoader());

            leased.close();
            assertEquals(callerClassLoader, Thread.currentThread().getContextClassLoader());
            verify(lease).close();
        }
    }
}
