package com.tectonica.collections;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class ConcurrentRefCounter<K>
{
	private final ConcurrentMap<K, Integer> map = new ConcurrentHashMap<K, Integer>();

	public int increase(K key)
	{
		while (true)
		{
			Integer count = map.get(key);
			if (count == null)
			{
				if (map.putIfAbsent(key, 1) == null)
					return 1;
			}
			else if (map.replace(key, count, count + 1))
				return count + 1;
		}
	}

	public int decrease(K key)
	{
		while (true)
		{
			Integer count = map.get(key);
			if (count == null)
				return -1;
			if (count == 1)
			{
				if (map.remove(key, 1))
					return 0;
			}
			else if (map.replace(key, count, count - 1))
				return count - 1;
		}
	}
}