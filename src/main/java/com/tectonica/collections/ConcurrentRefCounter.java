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