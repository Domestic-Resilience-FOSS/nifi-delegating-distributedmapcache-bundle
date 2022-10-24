/*
 * See the NOTICE file distributed with this work for additional information
 * regarding copyright ownership. Booz Allen Hamilton licenses this file to
 * You under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy
 * of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.nifi.controller.mapcache

import org.apache.nifi.controller.AbstractControllerService
import org.apache.nifi.distributed.cache.client.Deserializer
import org.apache.nifi.distributed.cache.client.DistributedMapCacheClient
import org.apache.nifi.distributed.cache.client.Serializer
import org.apache.nifi.processor.AbstractProcessor
import org.apache.nifi.processor.ProcessContext
import org.apache.nifi.processor.ProcessSession
import org.apache.nifi.processor.exception.ProcessException
import org.apache.nifi.util.TestRunner
import org.apache.nifi.util.TestRunners
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

class DelegatingDistributedMapCacheTest {
    TestRunner runner

    DelegatingDistributedMapCache client

    List<MockDistributedMapCacheClient> services

    @BeforeEach
    void before() {
        runner = TestRunners.newTestRunner(new AbstractProcessor() {
            @Override
            void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {

            }
        })

        services = [
            new MockDistributedMapCacheClient(name: "1.redis"),
            new MockDistributedMapCacheClient(name: "3.hbase"),
            new MockDistributedMapCacheClient(name: "2.cassandra")
        ]

        client = new DelegatingDistributedMapCache()
        runner.addControllerService("delegate", client)

        services.each { service ->
            runner.addControllerService(service.name, service)
            runner.setProperty(client, service.name, service.name)
            runner.enableControllerService(service)
        }

        runner.enableControllerService(client)

        runner.assertValid()
    }

    static final SERIALIZER = { String o, OutputStream out ->
        out.write(o.bytes)
    } as Serializer<String>

    static final DESERIALIZER = { bytes ->
        new String(bytes)
    } as Deserializer<String>

    @Test
    void testContainsKey() {
        services[2].data["test-key"] = "Hello, world"

        assert client.containsKey("test-key", SERIALIZER)
    }

    @Test
    void testGet() {
        final def EXPECTED = "Hello, world"
        services[2].data["test-key"] = EXPECTED

        assert client.get("test-key", SERIALIZER, DESERIALIZER) == EXPECTED
    }

    @Test
    void testGetAndPutIfAbsent() {
        final def VALUE = "Lorem ipsum"
        final def KEY = "test-key"
        services[0].data[KEY] = VALUE
        services[2].data[KEY] = VALUE

        assert client.getAndPutIfAbsent("test-key", VALUE, SERIALIZER, SERIALIZER, DESERIALIZER) == VALUE
        services.each { service ->
            assert service.data[KEY] == VALUE
        }
    }

    @Test
    void testPut() {
        final def VALUE = "Lorem ipsum"
        final def KEY = "test-key"

        client.put(KEY, VALUE, SERIALIZER, SERIALIZER)
        services.each { service ->
            assert service.data[KEY] == VALUE
        }
    }

    @Test
    void testPutIfAbsent() {
        final def VALUE = "Lorem ipsum"
        services[0].data["test-key"] = VALUE
        services[2].data["test-key"] = VALUE

        assert client.putIfAbsent("test-key", VALUE, SERIALIZER, SERIALIZER)
    }

    @Test
    void testRemove() {
        services[1].data["test-key"] = "Lorem ipsum"

        assert client.remove("test-key", SERIALIZER)
    }

    @Test
    void testRemoveByPattern() {
        final def KEY = "test-key"
        services.each { service ->
            service.data[KEY] = "Lorem ipsum"
        }

        assert client.removeByPattern("test-") == 1
    }

    @Test
    void testRequiresProperties() {
        TestRunner temp = TestRunners.newTestRunner(new AbstractProcessor() {
            @Override
            void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {

            }
        })

        def emptyClient = new DelegatingDistributedMapCache()
        temp.addControllerService("Test", emptyClient)

        def ex = null
        try {
            temp.enableControllerService(emptyClient)
        } catch (IllegalStateException i) {
            ex = i
        } finally {
            assert ex instanceof IllegalStateException
        }
    }

    @Test
    void testRequiresPropertyMustBeDMC() {
        TestRunner temp = TestRunners.newTestRunner(new AbstractProcessor() {
            @Override
            void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {

            }
        })

        def emptyClient = new DelegatingDistributedMapCache()
        temp.addControllerService("Test", emptyClient)
        temp.setProperty(emptyClient, "1.redis", "")

        def ex = null
        try {
            temp.enableControllerService(emptyClient)
        } catch (IllegalStateException i) {
            ex = i
        } finally {
            assert ex instanceof IllegalStateException
        }
    }

    class MockDistributedMapCacheClient extends AbstractControllerService implements DistributedMapCacheClient {
        Map data = [:]

        String name

        @Override
        def <K, V> boolean putIfAbsent(K k, V v, Serializer<K> serializer, Serializer<V> serializer1) throws IOException {
            if (data[k]) {
                false
            } else {
                data[k] = v
                true
            }
        }

        @Override
        def <K, V> V getAndPutIfAbsent(K k, V v, Serializer<K> serializer, Serializer<V> serializer1, Deserializer<V> deserializer) throws IOException {
            putIfAbsent(k, v, serializer, serializer1)
            return v
        }

        @Override
        def <K> boolean containsKey(K k, Serializer<K> serializer) throws IOException {
            return data.containsKey(k)
        }

        @Override
        def <K, V> void put(K k, V v, Serializer<K> serializer, Serializer<V> serializer1) throws IOException {
            data[k] = v
        }

        @Override
        def <K, V> V get(K k, Serializer<K> serializer, Deserializer<V> deserializer) throws IOException {
            data[k] as V
        }

        @Override
        void close() throws IOException {

        }

        @Override
        def <K> boolean remove(K k, Serializer<K> serializer) throws IOException {
            return data.remove(k) != null
        }

        @Override
        long removeByPattern(String s) throws IOException {
            def matched = data.keySet().findAll { k -> k.toString().startsWith(s) }
            matched.each { m -> data.remove(m) }
            matched.size()
        }
    }
}
