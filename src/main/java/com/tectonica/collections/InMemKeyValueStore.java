package com.tectonica.collections;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.tectonica.collections.ConcurrentMultimap;
import com.tectonica.util.SerializeUtil;

public class InMemKeyValueStore<K, V extends Serializable> extends KeyValueStore<K, V>
{
	protected class InMemDocument implements KeyValue<K, V>
	{
		private final K _key; // is never null
		private V _entry; // is never null

		public InMemDocument(K key, V entry)
		{
			if (key == null || entry == null)
				throw new NullPointerException();
			_key = key;
			_entry = entry;
		}

		@Override
		public K getKey()
		{
			return _key;
		}

		@Override
		public V getValue()
		{
			return _entry;
		}

		@Override
		public V getForWrite()
		{
			return SerializeUtil.copyOf(_entry); // TODO: replace with a more efficient implementation
		}

		@Override
		public void commit(V entry)
		{
			V oldEntry = _entry;
			_entry = entry;
			reindex(_key, oldEntry, entry);
		}
	}

	private final ConcurrentHashMap<K, KeyValue<K, V>> entries;
	private final ConcurrentHashMap<K, Lock> locks;
	private final List<InMemIndexImpl<?>> indices;

	public InMemKeyValueStore(KeyMapper<K, V> keyMapper)
	{
		super(keyMapper);
		this.entries = new ConcurrentHashMap<>();
		this.locks = new ConcurrentHashMap<>();
		this.indices = new ArrayList<>();
	}

	@Override
	public <F> Index<K, V, F> createIndex(String indexName, IndexMapper<V, F> mapFunc)
	{
		if (entries.size() > 0)
			throw new RuntimeException("adding indices on non-empty data set is not supported yet");

		InMemIndexImpl<F> index = new InMemIndexImpl<>(mapFunc, indexName);
		indices.add(index);
		return index;
	}

	@Override
	public void insert(K key, V entry)
	{
		KeyValue<K, V> existing = entries.putIfAbsent(key, new InMemDocument(key, entry));
		if (existing == null)
			reindex(key, null, entry);
		else
			throw new RuntimeException("attempted to insert entry with existing key " + key);
	}

	@Override
	public void truncate()
	{
		entries.clear();
		locks.clear();
		clearIndices();
	}

	@Override
	protected Set<K> getAllKeys()
	{
		return entries.keySet();
	}

	@Override
	protected KeyValue<K, V> getKeyValue(K key, Purpose purpose)
	{
		return entries.get(key);
	}

	@Override
	public Lock getWriteLock(K key)
	{
		Lock lock;
		Lock existing = locks.putIfAbsent(key, lock = new ReentrantLock());
		if (existing != null)
			lock = existing;
		return lock;
	}

	private void clearIndices()
	{
		for (InMemIndexImpl<?> index : indices)
			index.clear();
	}

	private void reindex(K key, V oldEntry, V newEntry)
	{
		for (int i = 0; i < indices.size(); i++)
		{
			InMemIndexImpl<?> index = indices.get(i);
			Object oldField = (oldEntry == null) ? null : index.mapFunc.getIndexedFieldOf(oldEntry);
			Object newField = index.mapFunc.getIndexedFieldOf(newEntry);
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

	/**
	 * straightforward in-memory implementation of an index
	 * 
	 * @author Zach Melamed
	 */
	public class InMemIndexImpl<F> extends Index<K, V, F>
	{
		private ConcurrentMultimap<Object, K> dictionary;

		public InMemIndexImpl(IndexMapper<V, F> mapFunc, String name)
		{
			super(mapFunc, name);
			this.dictionary = new ConcurrentMultimap<>();
		}

		@Override
		public Set<K> getKeysOf(F f)
		{
			return dictionary.get(f);
		}

		@Override
		public Collection<V> getEntriesOf(F f)
		{
			Collection<V> list = new ArrayList<>();
			Set<K> keys = getKeysOf(f);
			if (keys == null)
				return null;
			for (K key : keys)
			{
				KeyValue<K, V> kv = entries.get(key);
				if (kv != null) // would be very strange if we get null here
					list.add(kv.getValue());
			}
			return list;
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
}
