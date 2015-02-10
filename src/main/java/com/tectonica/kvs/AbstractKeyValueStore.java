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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.locks.Lock;

import com.tectonica.collections.ConcurrentMultimap;
import com.tectonica.kvs.Index.IndexMapper;

public abstract class AbstractKeyValueStore<K, V> implements KeyValueStore<K, V>
{
	protected final KeyMapper<K, V> keyMapper;

	/**
	 * creates a new key-value store manager (this doesn't indicate creation of a brand new storage unit (e.g. table), only a creation of
	 * this class, which manages a newly-created/pre-existing storage)
	 * 
	 * @param keyMapper
	 *            this optional parameter is suitable in situations where the key of an entry can be inferred from its value directly
	 *            (as opposed to when the key and value are stored separately). when provided, several convenience methods become applicable
	 */
	protected AbstractKeyValueStore(KeyMapper<K, V> keyMapper)
	{
		this.keyMapper = keyMapper;
		initializeCache();
	}

	protected void initializeCache()
	{
		cache = createCache();
		usingCache = (cache != null);
	}

	/* *********************************************************************************
	 * 
	 * GETTERS
	 * 
	 * many of the non-abstract methods here offer somewhat of a naive implementation.
	 * subclasses are welcome to override with their own efficient implementation.
	 * 
	 * *********************************************************************************
	 */

	/**
	 * Given a key, retrieves its paired value directly from the storage
	 */
	protected abstract V dbGet(K key);

	/**
	 * Retrieves the entries indicated by the given keys directly from the storage
	 * <p>
	 * See {@link #iteratorFor(Collection, boolean)} for the restrictions on the resulting iterator
	 */
	protected abstract Iterator<KeyValue<K, V>> dbOrderedIterator(Collection<K> keys);

	/**
	 * Returns ALL entries, bypassing cache entirely (read and write)
	 */
	@Override
	public abstract Iterator<KeyValue<K, V>> iterator();

	// ///////////////////////////////////////////////////////////////////////////////////////

	@Override
	public V get(K key)
	{
		return get(key, true);
	}

	@Override
	public V get(K key, boolean cacheResult)
	{
		if (!usingCache)
			return dbGet(key);

		V value = cache.get(key);
		if (value == null)
		{
			value = dbGet(key);
			if (cacheResult && value != null)
				cache.put(key, value);
		}
		return value;
	}

	/**
	 * Executes {@link #iteratorFor(Collection, boolean)} with {@code postponeCaching} set to false
	 */
	@Override
	public Iterator<KeyValue<K, V>> iteratorFor(final Collection<K> keys)
	{
		return iteratorFor(keys, false);
	}

	/**
	 * Returns an iterator for a list of entries whose keys were passed to the method
	 * <p>
	 * <b>IMPORTANT:</b> Unlike simple 'SELECT WHERE KEY IN (..)', the list returned here is guaranteed to be at the exact same order of the
	 * keys passed. Moreover, if a certain key is passed more than once, so will its corresponding entry. Clearly, if a key doesn't exist in
	 * the store, it will be skipped (i.e. the returning list will never contain nulls).
	 * 
	 * @param keys
	 *            ordered collection of keys to lookup (this is not necessarily a set, as keys can appear more than once in the collection)
	 * @param postponeCaching
	 *            indicates whether cache entries individually as each is read from the backend storage, or cache them all in a single call
	 *            at the end of the iteration (use only if you'll certainly iterate through the entire result)
	 */
	@Override
	public Iterator<KeyValue<K, V>> iteratorFor(final Collection<K> keys, final boolean postponeCaching)
	{
		if (keys.isEmpty())
			return Collections.emptyIterator();

		if (!usingCache)
			return dbOrderedIterator(keys);

		// we intend to use cached results of at least some of the keys passed. the general goal is to find out which keys are missing,
		// retrieve them separately from the storage, and then merge into a single list the cached and retrieved entries

		final Map<K, V> cachedValues = cache.get(keys);

		final Iterator<KeyValue<K, V>> dbIter;
		if (cachedValues.size() == keys.size())
			dbIter = Collections.emptyIterator(); // i.e. all keys were found in cache
		else
		{
			final Collection<K> uncachedKeys;
			if (cachedValues.size() == 0) // i.e. no key was found on cache
				uncachedKeys = keys;
			else
			{
				uncachedKeys = new ArrayList<>();
				for (K key : keys)
					if (!cachedValues.containsKey(key))
						uncachedKeys.add(key);
			}
			if (uncachedKeys.isEmpty())
				dbIter = Collections.emptyIterator(); // possible only when duplicate keys were passed as input
			else
				dbIter = dbOrderedIterator(uncachedKeys);
		}

		return new Iterator<KeyValue<K, V>>()
		{
			private Iterator<K> keysIter = keys.iterator();
			private KeyValue<K, V> dbNext = dbIter.hasNext() ? dbIter.next() : null;
			private KeyValue<K, V> nextItem = null;
			private Map<K, V> toCache = new HashMap<>();

			@Override
			public boolean hasNext()
			{
				if (nextItem != null)
					return true;

				while (keysIter.hasNext())
				{
					K key = keysIter.next();

					// try from db
					if (dbNext != null && dbNext.getKey().equals(key))
					{
						// cache it first (or mark for postponed caching)
						V value = dbNext.getValue();
						if (!postponeCaching)
							cache.put(key, value);
						else
							toCache.put(key, value);

						// take value and move db-pointer to next entry
						nextItem = dbNext;
						dbNext = dbIter.hasNext() ? dbIter.next() : null;
						return true;
					}

					// try from cache
					V value = cachedValues.get(key);
					if (value != null)
					{
						nextItem = KvsUtil.keyValueOf(key, value);
						return true;
					}
				}

				if (dbIter.hasNext())
					throw new RuntimeException("Internal error in cache-based iteration");

				if (postponeCaching && toCache != null)
				{
					if (!toCache.isEmpty())
						cache.put(toCache);
					toCache = null;
				}

				return false; // i.e. nextVal is null
			}

			@Override
			public KeyValue<K, V> next()
			{
				if (!hasNext())
					throw new NoSuchElementException();
				KeyValue<K, V> next = nextItem;
				nextItem = null;
				return next;
			}

			@Override
			public void remove()
			{
				throw new UnsupportedOperationException();
			}
		};
	}

	@Override
	public Iterator<K> keyIterator()
	{
		final Iterator<KeyValue<K, V>> iter = iterator();
		return new Iterator<K>()
		{
			@Override
			public boolean hasNext()
			{
				return iter.hasNext();
			}

			@Override
			public K next()
			{
				return iter.next().getKey();
			}

			@Override
			public void remove()
			{
				throw new UnsupportedOperationException();
			}
		};
	}

	@Override
	public Iterator<V> valueIterator()
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
	public Iterator<V> valueIteratorFor(Collection<K> keys)
	{
		return valueIteratorFor(keys, false);
	}

	@Override
	public Iterator<V> valueIteratorFor(Collection<K> keys, boolean postponeCaching)
	{
		if (keys.isEmpty())
			return Collections.emptyIterator();

		final Iterator<KeyValue<K, V>> iter = iteratorFor(keys, postponeCaching);
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
		return KvsUtil.iterateInto(keyIterator(), new HashSet<K>());
	}

	@Override
	public List<V> values()
	{
		return KvsUtil.iterateInto(valueIterator(), new ArrayList<V>());
	}

	@Override
	public List<V> valuesFor(Collection<K> keys)
	{
		if (keys.isEmpty())
			return Collections.emptyList();
		return KvsUtil.iterateInto(valueIteratorFor(keys, true), new ArrayList<V>());
	}

	@Override
	public boolean containsKey(K key)
	{
		// NOTE: the implementation here avoid serialization, but does caching. you'll probably want to override..
		return iteratorFor(Collections.singletonList(key)).hasNext();
	}

	/* *********************************************************************************
	 * 
	 * SETTERS (PROTOCOL)
	 * 
	 * *********************************************************************************
	 */

	/**
	 * an interface for managing a modification process of an existing entry. there are two types of such modification:
	 * <ul>
	 * <li>using {@link KeyValueStore#put(Object, Object)}: in such case only the {@link #dbPut(Object)} method will be invoked. it will be
	 * passed an updated value for an existing key.
	 * <li>using {@link KeyValueStore#update(Object, Updater)}: in such case first the {@link #getModifiableValue()} method will be invoked,
	 * generating an instance for the caller to safely modify, and then the {@link #dbPut(Object)} method will be invoked on that modified
	 * instance.
	 * </ul>
	 * both methods are invoked under the concurrency protection a lock provided with {@link KeyValueStore#getModificationLock(Object)}.
	 */
	protected interface Modifier<K, V>
	{
		/**
		 * Returns an instance that can be safely modified by the caller. During this modification, calls to {@link #getValue()} will return
		 * the unchanged value. If the instance was indeed modified by the caller, and no exception occurred in the process, the method
		 * {@link #dbPut(Object)} will be invoked.
		 * <p>
		 * NOTE: this method is called only on a locked entry
		 */
		V getModifiableValue();

		/**
		 * Makes the changes to an entry permanent. After this method finishes, calls to {@link KeyValueStore#get(Object, boolean)} will
		 * return the updated value.
		 * <p>
		 * NOTE: this method is called only on a locked entry
		 */
		void dbPut(V value);
	}

	protected static enum ModificationType
	{
		UPDATE, PUT;
	}

	/**
	 * required to return a (short-lived) instance of {@link Modifier} corresponding to a given key, or a null if the passed key can't be
	 * updated (because it doesn't exist, or for another reason). The returned instance is used for a one-time modification.
	 */
	protected abstract Modifier<K, V> getModifier(K key, ModificationType purpose);

	/**
	 * expected to return a global lock for a specific key (global means that it blocks other machines as well, not just the current
	 * instance). It is a feature of this framework that only one updater is allowed for an entry at each given moment, so whenever an
	 * updater thread starts the (non-atomic) process of updating the entry, all other attempts should be blocked.
	 */
	protected abstract Lock getModificationLock(K key);

	/* *********************************************************************************
	 * 
	 * SETTERS
	 * 
	 * *********************************************************************************
	 */

	protected abstract void dbInsert(K key, V value);

	/**
	 * inserts a new entry, whose key doesn't already exist in storage. it's a faster and more resource-efficient way to insert entries
	 * compared to {@link #put(Object, Object)} as it doesn't use any locking. do not use if you're not completely sure whether the key
	 * already exists. the behavior of the store in such case is undetermined and implementation-dependent.
	 */
	@Override
	public void add(K key, V value)
	{
		fireEvent(EventType.PreAdd, key, value);
		dbInsert(key, value);
		if (usingCache)
			cache.put(key, value);
	}

	/**
	 * inserts or updates an entry. if you're sure that the entry is new (i.e. its key doesn't already exist), use the more efficient
	 * {@link #add(Object, Object)} instead
	 */
	@Override
	public void put(K key, V value)
	{
		Lock lock = getModificationLock(key);
		lock.lock();
		try
		{
			Modifier<K, V> modifier = getModifier(key, ModificationType.PUT);
			if (modifier == null)
				add(key, value);
			else
			{
				fireEvent(EventType.PrePut, key, value);
				modifier.dbPut(value);
				if (usingCache)
					cache.put(key, value);
			}
		}
		finally
		{
			lock.unlock();
		}
	}

	/**
	 * insert and entry only if the key is not yet taken
	 * 
	 * @return
	 *         the newly added value, or null if nothing was inserted
	 */
	@Override
	public V putIfAbsent(K key, ValueGenerator<K, V> generator)
	{
		Lock lock = getModificationLock(key);
		lock.lock();
		try
		{
			if (containsKey(key))
				return null;

			V value = generator.generate(key);
			add(key, value);
			return value;
		}
		finally
		{
			lock.unlock();
		}
	}

	@Override
	public V update(K key, Updater<V> updater)
	{
		Lock lock = getModificationLock(key);
		lock.lock();
		try
		{
			Modifier<K, V> modifier = getModifier(key, ModificationType.UPDATE);
			if (modifier == null)
			{
				updater.entryNotFound();
				return null;
			}

			V value = modifier.getModifiableValue();
			if (value == null)
			{
				updater.entryNotFound();
				return null;
			}

			fireEvent(EventType.PreUpdate, key, value);
			updater.changed = updater.update(value);

			if (updater.changed)
			{
				fireEvent(EventType.PrePersistUpdate, key, value);
				modifier.dbPut(value);
				if (usingCache)
					cache.put(key, value);
			}

			updater.postPersist(value);

			return value;
		}
		finally
		{
			lock.unlock();
		}
	}

	@Override
	public int update(Collection<K> keys, Updater<V> updater)
	{
		int count = 0;
		for (K key : keys)
		{
			update(key, updater);
			if (updater.changed)
				count++;
			if (updater.stopped)
				break;
		}
		return count;
	}

	/* *********************************************************************************
	 * 
	 * SETTERS (CONVENIENCE)
	 * 
	 * *********************************************************************************
	 */

	/**
	 * convenience method to update all entries
	 */
	@Override
	public int updateAll(Updater<V> updater)
	{
		return update(keySet(), updater);
	}

	/**
	 * convenience method applicable when {@code keyMapper} is provided
	 * 
	 * @see {@link #update(Object, Updater)}
	 */
	@Override
	public V updateValue(V value, Updater<V> updater)
	{
		return update(keyMapper.getKeyOf(value), updater);
	}

	/**
	 * convenience method applicable when {@code keyMapper} is provided
	 * 
	 * @see {@link #add(Object, Object)}
	 */
	@Override
	public void addValue(V value)
	{
		add(keyMapper.getKeyOf(value), value);
	}

	/**
	 * convenience method applicable when {@code keyMapper} is provided
	 * 
	 * @see {@link #put(Object, Object)}
	 */
	@Override
	public void putValue(V value)
	{
		put(keyMapper.getKeyOf(value), value);
	}

	/* *********************************************************************************
	 * 
	 * DELETERS
	 * 
	 * *********************************************************************************
	 */

	protected abstract boolean dbDelete(K key);

	protected abstract int dbDeleteAll();

	@Override
	public boolean delete(K key)
	{
		if (usingCache)
			cache.delete(key);
		return dbDelete(key);
	}

	@Override
	public int deleteAll()
	{
		if (usingCache)
			cache.deleteAll();
		return dbDeleteAll();
	}

	/* *********************************************************************************
	 * 
	 * INDEXES
	 * 
	 * *********************************************************************************
	 */

	@Override
	public abstract <F> Index<K, V, F> createIndex(String indexName, IndexMapper<V, F> mapFunc);

	/* *********************************************************************************
	 * 
	 * CACHE
	 * 
	 * *********************************************************************************
	 */

	protected Cache<K, V> cache;
	protected boolean usingCache;

	protected static interface Cache<K, V>
	{
		V get(K key);

		Map<K, V> get(Collection<K> keys);

		void put(K key, V value);

		void put(Map<K, V> values);

		void delete(K key);

		void deleteAll();
	}

	/**
	 * overridden by subclasses that wish to support a caching mechanism
	 */
	protected Cache<K, V> createCache()
	{
		return null;
	}

	@Override
	public void clearCache()
	{
		if (usingCache)
			cache.deleteAll();
	}

	/* *********************************************************************************
	 * 
	 * EVENTS
	 * 
	 * *********************************************************************************
	 */

	protected ConcurrentMultimap<EventType, EventHandler<K, V>> handlers = new ConcurrentMultimap<>();

	@Override
	public void addListener(EventType type, EventHandler<K, V> handler)
	{
		handlers.put(type, handler);
	}

	protected void fireEvent(EventType type, K key, V value)
	{
		Set<EventHandler<K, V>> events = handlers.get(type);
		if (events != null)
			for (EventHandler<K, V> event : events)
				event.handle(type, key, value);
	}
}
