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
