/*
 * Copyright (C) 2014 Zach Melamed
 * 
 * Latest version available online at https://github.com/zach-m/tectonica-commons
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.tectonica.collections;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class ConcurrentMultimap<K, V>
{
	private ConcurrentHashMap<K, Set<V>> map = new ConcurrentHashMap<>();

	public int put(K key, V value)
	{
		return put(key, value, true);
	}

	public int put(K key, V value, boolean autoCreateKey)
	{
		Set<V> valuesSet;
		if (autoCreateKey)
		{
			HashSet<V> emptySet = new HashSet<V>();
			valuesSet = map.putIfAbsent(key, emptySet);
			if (valuesSet == null)
				valuesSet = emptySet;
		}
		else
		{
			valuesSet = map.get(key);
			if (valuesSet == null)
				return 0;
		}
		synchronized (valuesSet)
		{
			valuesSet.add(value);
		}
		return valuesSet.size();
	}

	public Set<V> get(K key)
	{
		Set<V> valuesSet = map.get(key);
		if (valuesSet == null)
			return null;
		synchronized (valuesSet)
		{
			return new HashSet<V>(valuesSet);
		}
	}

	public int remove(K key, V value)
	{
		Set<V> valuesSet = map.get(key);
		if (valuesSet == null)
			return 0;
		synchronized (valuesSet)
		{
			valuesSet.remove(value);
			if (valuesSet.isEmpty())
				map.remove(key);
			return valuesSet.size();
		}
	}

	public void removeFromAll(V value)
	{
		for (K key : map.keySet())
			remove(key, value);
	}

	public void clear()
	{
		map.clear();
	}
}
