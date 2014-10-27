package com.tectonica.collections;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.tectonica.util.SerializeUtil;

public class InMemKeyValueStore<K, V extends Serializable> extends KeyValueStore<K, V>
{
	protected class InMemKeyValueHandle implements KeyValueHandle<K, V>
	{
		private final K _key; // is never null
		private V _entry; // is never null

		public InMemKeyValueHandle(K key, V entry)
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
		public V getModifiableValue()
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
		
		@Override
		public void delete()
		{
			entries.remove(_key);
			reindex(_key, _entry, null);
		}
	}

	private final ConcurrentHashMap<K, KeyValueHandle<K, V>> entries;
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
		KeyValueHandle<K, V> existing = entries.putIfAbsent(key, new InMemKeyValueHandle(key, entry));
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
	public Iterator<V> entryIterator()
	{
		final Iterator<KeyValue<K, V>> iter = iterator();
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
				return iter.next().getValue();
			}

			@Override
			public void remove()
			{
				throw new UnsupportedOperationException();
			}
		};
	}

	@Override
	public Set<K> keySet()
	{
		return entries.keySet();
	}

	@Override
	protected KeyValueHandle<K, V> getHandle(K key, Purpose purpose)
	{
		return entries.get(key);
	}

	@Override
	protected Iterator<KeyValueHandle<K, V>> getHandles(Set<K> keySet, Purpose purpose)
	{
		return super.getHandles(keySet, purpose); // there isn't a more performant solution
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
			Object newField = (newEntry == null) ? null : index.mapFunc.getIndexedFieldOf(newEntry);
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
		public Iterator<K> keyIteratorOf(F f)
		{
			final Set<K> keySet = dictionary.get(f);
			if (keySet == null)
				return Collections.emptyIterator();
			return keySet.iterator();
		}

		@Override
		public Iterator<V> entryIteratorOf(F f)
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
}
