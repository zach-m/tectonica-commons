package com.tectonica.collections;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Simple, yet powerful, in-memory key-value store, that can serve for caching or as an interim solution before permanent storage is
 * introduced to the project. This container supports indexing, so that lookups can be performed faster. It is thread safe and uses locks
 * where needed to prevent write-inconsistency.
 * 
 * @author Zach Melamed
 */
public class ConcurrentBucket<K, V>
{
	private final ConcurrentHashMap<K, V> entries;
	private final List<Index<K, V, ?>> indices;
	private final KeyMapper<K, V> keyMapper;

	public static interface KeyMapper<K, V>
	{
		public K getKeyOf(V value);
	}

	public static interface IndexMapper<V, F>
	{
		public F getIndexedFieldOf(V entry);
	}

	abstract public static class Index<K, V, F>
	{
		protected final IndexMapper<V, F> mapFunc;

		protected Index(IndexMapper<V, F> mapFunc)
		{
			this.mapFunc = mapFunc;
		}

		abstract public Set<K> get(F f);

		public K getFirst(F f)
		{
			Set<K> set = get(f);
			if (set == null || set.size() == 0)
				return null;
			return set.iterator().next();
		}

		abstract protected void map(Object indexField, K toKey);

		abstract protected void unMap(Object indexField, K toKey);

		abstract protected void clear();
	}

	public ConcurrentBucket(KeyMapper<K, V> keyMapper)
	{
		this.entries = new ConcurrentHashMap<>();
		this.indices = new ArrayList<>();
		this.keyMapper = keyMapper;
	}

	public <F> Index<K, V, F> createIndex(IndexMapper<V, F> mapFunc)
	{
		if (entries.size() > 0)
			throw new RuntimeException("adding indices on non-empty data set is not supported yet");
		Index<K, V, F> index = new ConcurrentBucketIndexImpl<>(mapFunc);
		indices.add(index);
		return index;
	}

	// for advanced uses, not been tested yet..
	public <F> void addIndex(Index<K, V, F> index)
	{
		if (entries.size() > 0)
			throw new RuntimeException("adding indices on non-empty data set is not supported yet");
		indices.add(index);
	}

	public List<Index<K, V, ?>> allIndices()
	{
		return indices;
	}

	public V get(K key)
	{
		return entries.get(key);
	}

	public Collection<V> getAll()
	{
		return entries.values();
	}

	public boolean add(V entry)
	{
		K key = keyMapper.getKeyOf(entry);
		V existing = entries.putIfAbsent(key, entry);
		if (existing != null)
			return false;

		addEntryToIndices(entry, key);

		return true;
	}

	public void clear()
	{
		entries.clear();
		clearIndices();
	}

	private void clearIndices()
	{
		for (Index<K, V, ?> index : indices)
			index.clear();
	}

	private void addEntryToIndices(V addedEntry, K addedKey)
	{
		// addedKey is passed for efficiency, it should be clear that: addedKey = keyMapper.getKeyOf(addedEntry);
		for (Index<K, V, ?> index : indices)
		{
			Object indexedField = index.mapFunc.getIndexedFieldOf(addedEntry);
			if (indexedField != null)
				index.map(indexedField, addedKey);
		}
	}

	public static abstract class Updater<V>
	{
		private boolean stopped = false;

		protected void stopIteration()
		{
			stopped = true;
		}

		public abstract void update(V value);
	}

	public V update(K key, Updater<V> updater)
	{
		V entry = entries.get(key);
		if (entry == null)
			return null;
		return updateEntry(entry, updater, key);
	}

	public V updateEntry(V entry, Updater<V> updater)
	{
		return updateEntry(entry, updater, keyMapper.getKeyOf(entry));
	}

	private V updateEntry(V entry, Updater<V> updater, K key)
	{
		// entry is assumed to be non-null, key is assumed to be keyMapper.getKeyOf(entry)
		synchronized (entry)
		{
			Object[] fieldsBefore = new Object[indices.size()];
			for (int i = 0; i < indices.size(); i++)
				fieldsBefore[i] = indices.get(i).mapFunc.getIndexedFieldOf(entry);

			updater.update(entry);

			// update indices
			for (int i = 0; i < indices.size(); i++)
			{
				Index<K, V, ?> index = indices.get(i);
				Object oldField = fieldsBefore[i];
				Object newField = index.mapFunc.getIndexedFieldOf(entry);
				boolean valueChanged = ((oldField == null) != (newField == null)) || ((oldField != null) && !oldField.equals(newField));
				if (valueChanged)
				{
					if (oldField != null)
						index.unMap(oldField, key);
					if (newField != null)
						index.map(newField, key);
				}
			}

			return entry;
		}
	}

	public void updateAll(Updater<V> updater)
	{
		for (V entry : entries.values())
		{
			updateEntry(entry, updater);
			if (updater.stopped)
				break;
		}
	}

	public void updateSome(Collection<K> keys, Updater<V> updater)
	{
		for (K key : keys)
		{
			V entry = entries.get(key);
			if (entry != null)
			{
				updateEntry(entry, updater, key);
				if (updater.stopped)
					break;
			}
		}
	}

	/**
	 * This is the default implementation for an index. User may provide his own.
	 * 
	 * @author Zach Melamed
	 */
	public static class ConcurrentBucketIndexImpl<K, V, F> extends Index<K, V, F>
	{
		private ConcurrentMultimap<Object, K> dictionary;

		public ConcurrentBucketIndexImpl(IndexMapper<V, F> mapFunc)
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
