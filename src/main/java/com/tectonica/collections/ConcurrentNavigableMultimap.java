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
import java.util.TreeSet;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class ConcurrentNavigableMultimap<K, V> extends ConcurrentMultimap<K, V>
{
	public ConcurrentNavigableMultimap()
	{
		super();
	}

	public ConcurrentNavigableMultimap(boolean sortValueSets)
	{
		super(sortValueSets);
	}

	@Override
	protected void initMap()
	{
		map = new ConcurrentSkipListMap<>();
	}

	private ConcurrentNavigableMap<K, Set<V>> getMap()
	{
		return (ConcurrentNavigableMap<K, Set<V>>) map;
	}

	public Set<V> getRange(K fromKey, K toKey)
	{
		return getRange(fromKey, true, toKey, false);
	}

	/**
	 * returns a all the values included in range of keys. the range can be bound at both ends, or only at one
	 */
	public Set<V> getRange(K fromKey, boolean fromInclusive, K toKey, boolean toInclusive)
	{
		ConcurrentNavigableMap<K, Set<V>> subMap = subMapOfRange(fromKey, fromInclusive, toKey, toInclusive);

		if (subMap.size() == 0)
			return null;

		// we return a copy of the set, so that the caller won't be affected by future changes
		final Set<V> result = sortValueSets ? new TreeSet<V>() : new HashSet<V>();

		for (Set<V> valuesSet : subMap.values())
		{
			if (valuesSet == null)
				continue; // probably impossible
			synchronized (valuesSet)
			{
				result.addAll(valuesSet);
			}
		}

		return result;
	}

	private ConcurrentNavigableMap<K, Set<V>> subMapOfRange(K fromKey, boolean fromInclusive, K toKey, boolean toInclusive)
	{
		if (fromKey == null && toKey == null)
			throw new NullPointerException("both 'fromKey' and 'toKey' are null");

		final ConcurrentNavigableMap<K, Set<V>> subMap;
		if (fromKey != null && toKey != null)
			subMap = getMap().subMap(fromKey, fromInclusive, toKey, toInclusive);
		else if (fromKey != null)
			subMap = getMap().tailMap(fromKey, fromInclusive);
		else
			// if (toKey != null)
			subMap = getMap().headMap(toKey, toInclusive);

		return subMap;
	}

	// TODO: removeRange()
}
