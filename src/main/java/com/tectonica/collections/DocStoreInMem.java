package com.tectonica.collections;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.tectonica.util.SerializeUtil;

public class DocStoreInMem<K, V extends Serializable> extends DocStore<K, V>
{
	protected class InMemDocument implements Document<K, V>
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
		public V get()
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

	private final ConcurrentHashMap<K, Document<K, V>> entries;
	private final ConcurrentHashMap<K, Lock> locks;
	private final List<Index<K, V, ?>> indices;

	public DocStoreInMem(KeyMapper<K, V> keyMapper)
	{
		super(keyMapper);
		this.entries = new ConcurrentHashMap<>();
		this.locks = new ConcurrentHashMap<>();
		this.indices = new ArrayList<>();
	}

	@Override
	public <F> Index<K, V, F> createIndex(IndexMapper<V, F> mapFunc)
	{
		if (entries.size() > 0)
			throw new RuntimeException("adding indices on non-empty data set is not supported yet");

		Index<K, V, F> index = new InMemIndexImpl<>(mapFunc);
		indices.add(index);
		return index;
	}

	@Override
	public void insert(K key, V entry)
	{
		Document<K, V> existing = entries.putIfAbsent(key, new InMemDocument(key, entry));
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
	protected Set<K> getKeys()
	{
		return entries.keySet();
	}

	@Override
	protected Document<K, V> getDocument(K key)
	{
		return entries.get(key);
	}

	@Override
	public Lock lockForWrite(K key)
	{
		Lock lock;
		Lock existing = locks.putIfAbsent(key, lock = new ReentrantLock());
		if (existing != null)
			lock = existing;
		lock.lock();
		return lock;
	}

	private void clearIndices()
	{
		for (Index<K, V, ?> index : indices)
			index.clear();
	}

	private void reindex(K key, V oldEntry, V newEntry)
	{
		for (int i = 0; i < indices.size(); i++)
		{
			Index<K, V, ?> index = indices.get(i);
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
	public static class InMemIndexImpl<K, V, F> extends Index<K, V, F>
	{
		private ConcurrentMultimap<Object, K> dictionary;

		public InMemIndexImpl(IndexMapper<V, F> mapFunc)
		{
			super(mapFunc);
			this.dictionary = new ConcurrentMultimap<>();
		}

		@Override
		public Set<K> get(F f)
		{
			return dictionary.get(f);
		}

		@Override
		protected void map(Object indexField, K toKey)
		{
			dictionary.put(indexField, toKey);
		}

		@Override
		protected void unMap(Object indexField, K toKey)
		{
			dictionary.remove(indexField, toKey);
		}

		@Override
		protected void clear()
		{
			dictionary.clear();
		}
	}
}
