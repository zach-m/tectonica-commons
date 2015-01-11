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

package com.tectonica.kvs;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.tectonica.collections.ConcurrentMultimap;
import com.tectonica.util.SerializeUtil;

public class InMemKeyValueStore<K, V extends Serializable> extends AbstractKeyValueStore<K, V>
{
	private final ConcurrentHashMap<K, InMemEntry> entries;
	private final ConcurrentHashMap<K, Lock> locks;
	private final List<InMemIndexImpl<?>> indexes;

	/**
	 * creates an in-memory data store, suitable mostly for development.
	 * 
	 * @param keyMapper
	 *            this optional parameter is suitable in situations where the key of an entry can be inferred from its value directly
	 *            (as opposed to when the key and value are stored separately). when provided, several convenience methods become applicable
	 */
	public InMemKeyValueStore(KeyMapper<K, V> keyMapper)
	{
		super(keyMapper);
		this.entries = new ConcurrentHashMap<>();
		this.locks = new ConcurrentHashMap<>();
		this.indexes = new ArrayList<>();
	}

	protected class InMemEntry implements Modifier<K, V>, KeyValue<K, V>
	{
		private final K _key; // never null
		private V _value; // never null

		public InMemEntry(K key, V value)
		{
			if (key == null || value == null)
				throw new NullPointerException();
			_key = key;
			_value = value;
		}

		@Override
		public K getKey()
		{
			return _key;
		}

		@Override
		public V getValue()
		{
			return _value;
		}

		@Override
		public V getModifiableValue()
		{
			return SerializeUtil.copyOf(_value); // TODO: replace with a more efficient implementation
//			return KryoUtil.copyOf(_value);
		}

		@Override
		public void dbPut(V value)
		{
			V oldEntry = _value;
			_value = value;
			reindex(_key, oldEntry, value);
		}
	}

	/***********************************************************************************
	 * 
	 * GETTERS
	 * 
	 ***********************************************************************************/

	@Override
	protected V dbGet(K key)
	{
		KeyValue<K, V> kv = entries.get(key);
		if (kv == null)
			return null;
		return kv.getValue();
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	public Iterator<KeyValue<K, V>> iterator()
	{
		return (Iterator) entries.values().iterator();
	}

	@Override
	public Iterator<K> keyIterator()
	{
		return entries.keySet().iterator();
	}

	@Override
	protected Iterator<KeyValue<K, V>> dbOrderedIterator(Collection<K> keys)
	{
		List<KeyValue<K, V>> list = new ArrayList<>();
		for (K key : keys)
		{
			KeyValue<K, V> kv = entries.get(key);
			if (kv != null)
				list.add(kv);
		}
		return list.iterator();
	}

	@Override
	public Set<K> keySet()
	{
		return entries.keySet();
	}

	@Override
	public boolean containsKey(K key)
	{
		return entries.containsKey(key);
	}

	/***********************************************************************************
	 * 
	 * SETTERS (UTILS)
	 * 
	 ***********************************************************************************/

	@Override
	protected Modifier<K, V> getModifier(K key, ModificationType purpose)
	{
		return entries.get(key);
	}

	@Override
	public Lock getModificationLock(K key)
	{
		// TODO: in this simplistic implementation we always increase the size of the 'locks' map
		// if important, use the AutoEvictMap here
		Lock lock;
		Lock existing = locks.putIfAbsent(key, lock = new ReentrantLock());
		if (existing != null)
			lock = existing;
		return lock;
	}

	/***********************************************************************************
	 * 
	 * SETTERS
	 * 
	 ***********************************************************************************/

	@Override
	protected void dbInsert(K key, V value)
	{
		Modifier<K, V> existing = entries.putIfAbsent(key, new InMemEntry(key, value));
		if (existing == null)
			reindex(key, null, value);
		else
			throw new RuntimeException("attempted to insert entry with existing key " + key);
	}

	/***********************************************************************************
	 * 
	 * DELETERS
	 * 
	 ***********************************************************************************/

	@Override
	protected boolean dbDelete(K key)
	{
		InMemEntry removed = null;
		if (indexes.size() == 0)
			removed = entries.remove(key); // without indexes to update, this is a primitive operation
		else
		{
			KeyValue<K, V> kv = entries.get(key);
			if (kv != null)
			{
				V oldValue = kv.getValue();
				removed = entries.remove(key);
				reindex(key, oldValue, null);
			}
		}
		return (removed != null);
	}

	@Override
	protected int dbDeleteAll()
	{
		int removed = entries.size();
		entries.clear();
		locks.clear();
		clearIndices();
		return removed;
	}

	/***********************************************************************************
	 * 
	 * INDEXES
	 * 
	 ***********************************************************************************/

	@Override
	public <F> Index<K, V, F> createIndex(String indexName, IndexMapper<V, F> mapper)
	{
		if (entries.size() > 0)
			throw new RuntimeException("adding indexes on non-empty data set is not supported yet");

		InMemIndexImpl<F> index = new InMemIndexImpl<>(mapper, indexName);
		indexes.add(index);
		return index;
	}

	/**
	 * straightforward in-memory implementation of an index
	 * 
	 * @author Zach Melamed
	 */
	public class InMemIndexImpl<F> extends Index<K, V, F>
	{
		private ConcurrentMultimap<Object, K> dictionary;

		public InMemIndexImpl(IndexMapper<V, F> mapper, String name)
		{
			super(mapper, name);
			this.dictionary = new ConcurrentMultimap<>();
		}

		@Override
		public Iterator<KeyValue<K, V>> iteratorOf(F f)
		{
			final Iterator<K> iter = keyIteratorOf(f);
			return new Iterator<KeyValue<K, V>>()
			{
				@Override
				public boolean hasNext()
				{
					return iter.hasNext();
				}

				@Override
				public KeyValue<K, V> next()
				{
					K key = iter.next();
					return keyValueOf(key, entries.get(key).getValue());
				}

				@Override
				public void remove()
				{
					throw new UnsupportedOperationException();
				}
			};
		}

		@Override
		public Iterator<K> keyIteratorOf(F f)
		{
			Set<K> keys = dictionary.get(f);
			if (keys == null)
				return Collections.emptyIterator();
			return keys.iterator();
		}

		@Override
		public Iterator<V> valueIteratorOf(F f)
		{
			final Iterator<K> iter = keyIteratorOf(f);
			return new Iterator<V>()
			{
				@Override
				public boolean hasNext()
				{
					return iter.hasNext();
				}

				@Override
				public V next()
				{
					return entries.get(iter.next()).getValue();
				}

				@Override
				public void remove()
				{
					throw new UnsupportedOperationException();
				}
			};
		}

		protected void map(Object indexField, K toKey)
		{
			dictionary.put(indexField, toKey);
		}

		protected void unMap(Object indexField, K toKey)
		{
			dictionary.remove(indexField, toKey);
		}

		protected void clear()
		{
			dictionary.clear();
		}
	}

	private void clearIndices()
	{
		for (InMemIndexImpl<?> index : indexes)
			index.clear();
	}

	private void reindex(K key, V oldEntry, V newEntry)
	{
		for (int i = 0; i < indexes.size(); i++)
		{
			InMemIndexImpl<?> index = indexes.get(i);
			Object oldField = (oldEntry == null) ? null : index.mapper.getIndexedFieldOf(oldEntry);
			Object newField = (newEntry == null) ? null : index.mapper.getIndexedFieldOf(newEntry);
			boolean valueChanged = ((oldField == null) != (newField == null)) || ((oldField != null) && !oldField.equals(newField));
			if (valueChanged)
			{
				if (oldField != null)
					index.unMap(oldField, key);
				if (newField != null)
					index.map(newField, key);
			}
		}
	}
}
