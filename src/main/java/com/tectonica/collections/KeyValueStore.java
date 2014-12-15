package com.tectonica.collections;

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

import com.tectonica.collections.KeyValueStore.KeyValue;

/**
 * Simple, yet powerful, framework (and approach) for handling a key-value store. It allows for multiple concurrent readers but a single
 * concurrent writer of each entry. It also provides a read-before-update mechanism which makes life simpler and keeps data consistent.
 * Indexing is also supported as part of the framework. The interface is intuitive and straightforward. This class itself is abstract and
 * subclasses are included for several backend persistence engines, including in-memory. (which is great for development).
 * <p>
 * The usage of this framework would probably yield the most benefit when used to prototype a data model, where changes are frequent and not
 * backwards-compatible. However, in more than a few scenarios, this framework can be used in production. The heavy-lifting is done by the
 * backend database either way.
 * 
 * @author Zach Melamed
 */
public abstract class KeyValueStore<K, V> implements Iterable<KeyValue<K, V>>
{
	public interface KeyValue<K, V>
	{
		K getKey();

		V getValue();
	}

	public interface KeyMapper<K, V>
	{
		public K getKeyOf(V value);
	}

	protected final KeyMapper<K, V> keyMapper;

	/**
	 * creates a new key-value store manager
	 * 
	 * @param keyMapper
	 *            this optional parameter is suitable in situations where the key of an entry can be inferred from its value directly
	 *            (as opposed to when the key and value are stored separately). when provided, several convenience methods become applicable
	 */
	protected KeyValueStore(KeyMapper<K, V> keyMapper)
	{
		this.keyMapper = keyMapper;
		cache = createCache();
		usingCache = (cache != null);
	}

	/***********************************************************************************
	 * 
	 * CACHE
	 * 
	 ***********************************************************************************/

	protected static interface Cache<K, V>
	{
		V get(K key);

		Map<K, V> get(Collection<K> keys);

		void put(K key, V value);

		void put(Map<K, V> values);

		void delete(K key);

		void truncate();
	}

	protected Cache<K, V> createCache()
	{
		return null;
	}

	public void clearCache()
	{
		if (usingCache)
			cache.truncate();
	}

	protected final Cache<K, V> cache;
	protected final boolean usingCache;

	/***********************************************************************************
	 * 
	 * GETTERS
	 * 
	 * many of the non-abstract methods here offer somewhat of a naive implementation.
	 * subclasses are welcome to override with their own efficient implementation.
	 * 
	 * IMPORTANT: implementations are responsible to operate the cache consistently
	 * 
	 ***********************************************************************************/

	protected abstract V dbRead(K key);

	protected abstract Iterator<KeyValue<K, V>> dbIterate(Collection<K> keys);

	@Override
	public abstract Iterator<KeyValue<K, V>> iterator(); // gets ALL entries, bypasses cache

	// ///////////////////////////////////////////////////////////////////////////////////////

	public V get(K key)
	{
		if (!usingCache)
			return dbRead(key);

		V value = cache.get(key);
		if (value == null)
		{
			value = dbRead(key);
			if (value != null)
				cache.put(key, value);
		}
		return value;
	}

//	public Iterator<KeyValue<K, V>> iteratorFor(final Collection<K> keys)
//	{
//		return iteratorFor(keys, false);
//	}

	public Iterator<KeyValue<K, V>> iteratorFor(final Collection<K> keys, final boolean postponeCaching)
	{
		if (keys.isEmpty())
			return Collections.emptyIterator();

		if (!usingCache)
			return dbIterate(keys);

		final Map<K, V> cachedValues = cache.get(keys);

		final Iterator<KeyValue<K, V>> dbIter;
		if (cachedValues.size() == keys.size())
			dbIter = Collections.emptyIterator(); // all keys were found in cache
		else
		{
			final Collection<K> uncachedKeys;
			if (cachedValues.size() == 0) // no key was found on cache
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
				dbIter = dbIterate(uncachedKeys);
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
						nextItem = keyValueOf(key, value);
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

//	public Iterator<V> valueIteratorFor(Collection<K> keys)
//	{
//		return valueIteratorFor(keys, false);
//	}

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

	public Set<K> keySet()
	{
		return iterateInto(keyIterator(), new HashSet<K>());
	}

	public List<V> values()
	{
		return iterateInto(valueIterator(), new ArrayList<V>());
	}

	public List<V> valuesFor(Collection<K> keys)
	{
		if (keys.isEmpty())
			return Collections.emptyList();
		return iterateInto(valueIteratorFor(keys, true), new ArrayList<V>());
	}

	/***********************************************************************************
	 * 
	 * SETTERS (PROTOCOL)
	 * 
	 * IMPORTANT: implementations are responsible to operate the cache consistently
	 * 
	 ***********************************************************************************/

	/**
	 * an interface for managing a modification process of an existing entry. there are two types of such modification:
	 * <ul>
	 * <li>using {@link KeyValueStore#replace(Object, Object)}: in such case only the {@link #dbWrite(Object)} method will be invoked. it
	 * will be passed an updated value for an existing key.
	 * <li>using {@link KeyValueStore#update(Object, Updater)}: in such case first the {@link #getModifiableValue()} method will be invoked,
	 * generating an instance for the caller to safely modify, and then the {@link #dbWrite(Object)} method will be invoked on that modified
	 * instance.
	 * </ul>
	 * both methods are invoked under the concurrency protection a lock provided with {@link KeyValueStore#getModificationLock(Object)}.
	 */
	protected interface Modifier<K, V>
	{
		/**
		 * Returns an instance that can be safely modified by the caller. During this modification, calls to {@link #getValue()} will return
		 * the unchanged value. If the instance was indeed modified by the caller, and no exception occurred in the process, the method
		 * {@link #dbWrite(Object)} will be invoked.
		 * <p>
		 * NOTE: this method is called only on a locked entry
		 */
		V getModifiableValue();

		/**
		 * Makes the changes to an entry permanent. After this method finishes, calls to {@link #getValue()} will return the updated value.
		 * <p>
		 * NOTE: this method is called only on a locked entry
		 */
		void dbWrite(V value);
	}

	protected static enum ModificationType
	{
		UPDATE, REPLACE;
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

	public static abstract class Updater<V>
	{
		private boolean stopped = false;
		private boolean changed = false;

		protected void stopIteration()
		{
			stopped = true;
		}

		public boolean isChanged()
		{
			return changed;
		}

		/**
		 * method inside which an entry may be safely modified with no concurrency or shared-state concerns
		 * 
		 * @param value
		 *            thread-safe entry on which the modifications are to be performed. never a null.
		 * @return
		 *         true if the method indeed changed the entry
		 */
		public abstract boolean update(V value);

		/**
		 * executes after the persistence has happened and getters return the new value. however, the entry is still locked at that point
		 * and can be addressed without any concern that another updater tries to start a modification.
		 * <p>
		 * IMPORTANT: do not make any modifications to the passed entry inside this method
		 */
		public void postCommit(V value)
		{}

		/**
		 * called if an update process was requested on a non existing key
		 */
		public void entryNotFound()
		{}
	}

	/***********************************************************************************
	 * 
	 * SETTERS
	 * 
	 ***********************************************************************************/

	/**
	 * inserts a new entry, whose key doesn't already exist in storage. it's a faster and more resource-efficient way to insert entries
	 * compared to {@link #replace(Object, Object)} as it doesn't use any locking. do not use if you're not completely sure whether the key
	 * already exists. the behavior of the store in such case is undetermined and implementation-dependent.
	 */
	public void insert(K key, V value)
	{
		fireEvent(EventType.PreInsert, key, value);
		dbInsert(key, value);
		if (usingCache)
			cache.put(key, value);
	}

	protected abstract void dbInsert(K key, V value);

	/**
	 * inserts or updates an entry. if you're sure that the entry is new (i.e. its key doesn't already exist), use the more efficient
	 * {@link #dbInsert(Object, Object)} instead
	 */
	public void replace(K key, V value)
	{
		Lock lock = getModificationLock(key);
		lock.lock();
		try
		{
			Modifier<K, V> modifier = getModifier(key, ModificationType.REPLACE);
			if (modifier == null)
				insert(key, value);
			else
			{
				fireEvent(EventType.PreReplace, key, value);
				modifier.dbWrite(value);
				if (usingCache)
					cache.put(key, value);
			}
		}
		finally
		{
			lock.unlock();
		}
	}

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
				fireEvent(EventType.PreCommit, key, value);
				modifier.dbWrite(value);
				if (usingCache)
					cache.put(key, value);
			}

			updater.postCommit(value);

			return value;
		}
		finally
		{
			lock.unlock();
		}
	}

	public void update(Collection<K> keys, Updater<V> updater)
	{
		for (K key : keys)
		{
			update(key, updater);
			if (updater.stopped)
				break;
		}
	}

	/***********************************************************************************
	 * 
	 * SETTERS (CONVENIENCE)
	 * 
	 ***********************************************************************************/

	/**
	 * convenience method to update all entries
	 */
	public void updateAll(Updater<V> updater)
	{
		update(keySet(), updater);
	}

	/**
	 * convenience method applicable when {@code keyMapper} is provided
	 * 
	 * @see {@link #update(Object, Updater)}
	 */
	public V updateValue(V value, Updater<V> updater)
	{
		return update(keyMapper.getKeyOf(value), updater);
	}

	/**
	 * convenience method applicable when {@code keyMapper} is provided
	 * 
	 * @see {@link #dbInsert(Object, Object)}
	 */
	public void insertValue(V value)
	{
		dbInsert(keyMapper.getKeyOf(value), value);
	}

	/**
	 * convenience method applicable when {@code keyMapper} is provided
	 * 
	 * @see {@link #replace(Object, Object)}
	 */
	public void replaceValue(V value)
	{
		replace(keyMapper.getKeyOf(value), value);
	}

	/***********************************************************************************
	 * 
	 * DELETERS
	 * 
	 ***********************************************************************************/

	protected abstract void dbDelete(K key);

	protected abstract void dbTruncate();

	public void delete(K key)
	{
		if (usingCache)
			cache.delete(key);
		dbDelete(key);
	}

	public void truncate()
	{
		if (usingCache)
			cache.truncate();
		dbTruncate();
	}

	/***********************************************************************************
	 * 
	 * EVENTS
	 * 
	 ***********************************************************************************/

	protected ConcurrentMultimap<EventType, EventHandler<K, V>> handlers = new ConcurrentMultimap<>();

	public static enum EventType
	{
		PreUpdate, PreCommit, PreInsert, PreReplace;
	}

	public interface EventHandler<K, V>
	{
		public void handle(EventType type, K key, V value);
	}

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

	/***********************************************************************************
	 * 
	 * INDEXES
	 * 
	 ***********************************************************************************/

	public abstract <F> Index<K, V, F> createIndex(String indexName, IndexMapper<V, F> mapFunc);

	public interface IndexMapper<V, F>
	{
		public F getIndexedFieldOf(V value);
	}

	public static abstract class Index<K, V, F>
	{
		protected final IndexMapper<V, F> mapper;
		protected final String name;

		protected Index(IndexMapper<V, F> mapper, String name)
		{
			this.mapper = mapper;
			this.name = name;
		}

		// ///////////////////////////////////////////////////////////////////////////////////////

		public abstract Iterator<KeyValue<K, V>> iteratorOf(F f);

		public abstract Iterator<K> keyIteratorOf(F f);

		public abstract Iterator<V> valueIteratorOf(F f);

		// ///////////////////////////////////////////////////////////////////////////////////////

		public boolean keyExistsOf(F f)
		{
			Iterator<K> iter = keyIteratorOf(f);
			return (iter.hasNext());
		}

		public Set<K> keySetOf(F f)
		{
			return iterateInto(keyIteratorOf(f), new HashSet<K>());
		}

		public List<V> valuesOf(F f)
		{
			return iterateInto(valueIteratorOf(f), new ArrayList<V>());
		}

		public List<KeyValue<K, V>> entriesOf(F f)
		{
			return iterateInto(iteratorOf(f), new ArrayList<KeyValue<K, V>>());
		}

		public Iterable<KeyValue<K, V>> asIterableOf(F f)
		{
			return iterableOf(iteratorOf(f));
		}

		public Iterable<K> asKeyIterableOf(F f)
		{
			return iterableOf(keyIteratorOf(f));
		}

		public Iterable<V> asValueIterableOf(F f)
		{
			return iterableOf(valueIteratorOf(f));
		}

		public KeyValue<K, V> getFirstEntry(F f)
		{
			Iterator<KeyValue<K, V>> iter = iteratorOf(f);
			if (iter.hasNext())
				return iter.next();
			return null;
		}

		public K getFirstKey(F f)
		{
			Iterator<K> iter = keyIteratorOf(f);
			if (iter.hasNext())
				return iter.next();
			return null;
		}

		public V getFirstValue(F f)
		{
			Iterator<V> iter = valueIteratorOf(f);
			if (iter.hasNext())
				return iter.next();
			return null;
		}

		public String getName()
		{
			return name;
		}
	}

	/***********************************************************************************
	 * 
	 * INTERNAL UTILS
	 * 
	 ***********************************************************************************/

	protected KeyValue<K, V> keyValueOf(final K key, final V value)
	{
		return new KeyValue<K, V>()
		{
			@Override
			public K getKey()
			{
				return key;
			}

			@Override
			public V getValue()
			{
				return value;
			}
		};
	}

	protected static <R, T extends Collection<R>> T iterateInto(Iterator<R> iter, T collection)
	{
		while (iter.hasNext())
			collection.add(iter.next());
		return collection;
	}

	protected static <T> Iterable<T> iterableOf(final Iterator<T> iter)
	{
		return new Iterable<T>()
		{
			@Override
			public Iterator<T> iterator()
			{
				return iter;
			}
		};
	}
}
