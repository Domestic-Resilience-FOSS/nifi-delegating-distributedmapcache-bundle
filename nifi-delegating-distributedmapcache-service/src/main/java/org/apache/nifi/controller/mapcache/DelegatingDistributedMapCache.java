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

package org.apache.nifi.controller.mapcache;

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.distributed.cache.client.Deserializer;
import org.apache.nifi.distributed.cache.client.DistributedMapCacheClient;
import org.apache.nifi.distributed.cache.client.Serializer;
import org.apache.nifi.processor.exception.ProcessException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@CapabilityDescription("Provides a Distributed Map Cache Client (DMC) that wraps other DMCs. The naming convention " +
	"for the dynamic properties is <integer>.<text_description> such as 1.redis or 2.cassandra.")
@Tags({"map", "cache"})
public class DelegatingDistributedMapCache extends AbstractControllerService implements DistributedMapCacheClient {
	private List<DistributedMapCacheClient> clients;

	@OnEnabled
	public void onEnabled(ConfigurationContext context) {
		Map<Integer, DistributedMapCacheClient> caches = context
			.getProperties()
			.keySet()
			.stream()
			.filter(PropertyDescriptor::isDynamic)
			.collect(Collectors.toMap(prop -> {
				String[] parts = prop.getName().split("\\.");
				return Integer.valueOf(parts[0]);
			}, prop -> context.getProperty(prop).asControllerService(DistributedMapCacheClient.class)));
		clients = caches.keySet()
			.stream()
			.sorted()
			.map(caches::get)
			.collect(Collectors.toList());
	}

	@OnDisabled
	public void onDisabled() {
		clients = null;
	}

	@Override
	protected Collection<ValidationResult> customValidate(ValidationContext context) {
		List<ValidationResult> results = new ArrayList<>();

		List<PropertyDescriptor> dynamic = context
			.getProperties()
			.keySet()
			.stream()
			.filter(PropertyDescriptor::isDynamic)
			.collect(Collectors.toList());

		if (dynamic.isEmpty()) {
			results.add(new ValidationResult.Builder().valid(false)
				.explanation("One or more dynamic properties must be added.").build());
		}

		dynamic.forEach(descriptor -> {
			if (!context.getProperty(descriptor).isSet()) {
				results.add(new ValidationResult.Builder()
					.subject(descriptor.getName())
					.explanation("The value must be set to a DistributeMapCacheClient instance.")
					.valid(false).build());
			}
		});

		return results;
	}

	@Override
	protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(String name) {
		String[] parts = name.split("\\.");
		if (parts.length < 2) {
			throw new ProcessException(String.format("%s is an invalid name; The name must start with a number, " +
				"followed by a period and additional text.", name));
		}
		return new PropertyDescriptor.Builder()
			.name(name)
			.identifiesControllerService(DistributedMapCacheClient.class)
			.addValidator(Validator.VALID)
			.dynamic(true)
			.build();
	}

	/**
	 * The behavior of the return value here is a slight break with the interface's documented contract. As this
	 * component wraps multiple map caches, it will return true if <strong>any</strong>, of them return true. It cannot
	 * therefore tell the user which ones returned true, just that some or all were updated.
	 *
	 * @param <K>             type of key
	 * @param <V>             type of value
	 * @param key             the key for into the map
	 * @param value           the value to add to the map if and only if the key is absent
	 * @param keySerializer   key serializer
	 * @param valueSerializer value serializer
	 * @return True if the value was added to any caches.
	 * @throws IOException
	 */
	@Override
	public <K, V> boolean putIfAbsent(K k, V v, Serializer<K> keySerializer, Serializer<V> valueSerializer) throws IOException {
		boolean retVal = false;
		for (DistributedMapCacheClient client : clients) {
			boolean result = client.putIfAbsent(k, v, keySerializer, valueSerializer);
			if (result) {
				retVal = result;
			}
		}

		return retVal;
	}

	@Override
	public <K, V> V getAndPutIfAbsent(K k, V v, Serializer<K> keySerializer, Serializer<V> valueSerializer,
									  Deserializer<V> deserializer) throws IOException {
		V result = null;

		for (DistributedMapCacheClient client : clients) {
			V temp = client.getAndPutIfAbsent(k, v, keySerializer, valueSerializer, deserializer);
			if (temp != null) {
				result = temp;
			}
		}

		return result;
	}

	@Override
	public <K> boolean containsKey(K k, Serializer<K> serializer) throws IOException {
		boolean result = false;
		for (DistributedMapCacheClient client : clients) {
			if (client.containsKey(k, serializer)) {
				result = true;
			}
		}
		return result;
	}

	@Override
	public <K, V> void put(K k, V v, Serializer<K> keySerializer, Serializer<V> valueSerializer) throws IOException {
		for (DistributedMapCacheClient client : clients) {
			client.put(k, v, keySerializer, valueSerializer);
		}
	}

	@Override
	public <K, V> V get(K k, Serializer<K> serializer, Deserializer<V> deserializer) throws IOException {
		V result = null;
		for (DistributedMapCacheClient client : clients) {
			V temp = client.get(k, serializer, deserializer);
			if (temp != null) {
				result = temp;
				break;
			}
		}

		return result;
	}

	@Override
	public void close() throws IOException {
		//NOOP
	}

	@Override
	public <K> boolean remove(K k, Serializer<K> serializer) throws IOException {
		boolean result = false;

		for (DistributedMapCacheClient client : clients) {
			if (client.remove(k, serializer)) {
				result = true;
			}
		}

		return result;
	}

	@Override
	public long removeByPattern(String s) throws IOException {
		List<Long> values = new ArrayList<>();

		for (DistributedMapCacheClient client : clients) {
			long count = client.removeByPattern(s);
			values.add(count);
		}

		return (long)values.stream()
			.mapToLong(Long::longValue)
			.summaryStatistics()
			.getAverage();
	}
}
