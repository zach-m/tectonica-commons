package com.tectonica.collections;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
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

	public interface KeyValueHandle<K, V> extends KeyValue<K, V>
	{
		/**
		 * Returns an instance that can be safely modified by the caller. During this modification, calls to {@link #getValue()} will return
		 * the unchanged value. If the instance was indeed modified by the caller, and no exception occurred in the process, the method
		 * {@link #commit(Object)} will be invoked.
		 * <p>
		 * NOTE: this method is called only on a locked entry
		 */
		V getModifiableValue();

		/**
		 * Makes the changes to an entry permanent. After this method finishes, calls to {@link #getValue()} will return the updated
		 * value.
		 * <p>
		 * NOTE: this method is called only on a locked entry
		 */
		void commit(V entry);
	}

	public interface KeyMapper<K, V>
	{
		public K getKeyOf(V value);
	}

	public interface IndexMapper<V, F>
	{
		public F getIndexedFieldOf(V entry);
	}

	public static abstract class Index<K, V, F>
	{
		protected final IndexMapper<V, F> mapFunc;
		protected final String name;

		protected Index(IndexMapper<V, F> mapFunc, String name)
		{
			this.mapFunc = mapFunc;
			this.name = name;
		}

		// ///////////////////////////////////////////////////////////////////////////////////////

		public abstract Iterator<K> keyIteratorOf(F f);

		public abstract Iterator<V> entryIteratorOf(F f);

		// ///////////////////////////////////////////////////////////////////////////////////////

		public Set<K> keySetOf(F f)
		{
			return KeyValueStore.iterateInto(keyIteratorOf(f), new HashSet<K>());
		}

		public List<V> entriesOf(F f)
		{
			return KeyValueStore.iterateInto(entryIteratorOf(f), new ArrayList<V>());
		}

		public Iterable<K> asKeyIterableOf(F f)
		{
			return KeyValueStore.iterableOf(keyIteratorOf(f));
		}

		public Iterable<V> asEntryIterableOf(F f)
		{
			return KeyValueStore.iterableOf(entryIteratorOf(f));
		}

		public K getFirstKey(F f)
		{
			Iterator<K> iter = keyIteratorOf(f);
			if (iter.hasNext())
				return iter.next();
			return null;
		}

		public V getFirstEntry(F f)
		{
			Iterator<V> iter = entryIteratorOf(f);
			if (iter.hasNext())
				return iter.next();
			return null;
		}

		public String getName()
		{
			return name;
		}
	}

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
		 * method inside which an entry may be safely modified with no concurrency concerns
		 * 
		 * @param entry
		 *            thread-safe entry on which the modifications are to be performed. never a null.
		 * @return
		 *         true if the method indeed changed the entry
		 */
		public abstract boolean update(V entry);

		/**
		 * executes after the persistence has happened and getters return the new value. however, the entry is still locked at that point
		 * and can be addressed without any concern that another updater tries to start a modification.
		 * <p>
		 * IMPORTANT: do not make any modifications to the passed entry inside this method
		 */
		public void postCommit(V entry)
		{}

		/**
		 * called if an update process was requested on a non existing key
		 */
		public void entryNotFound()
		{}
	}

	public static enum Purpose
	{
		READ, MODIFY, REPLACE;
	}

	// ////////////////////////////////////////////////////////////////////////////////////////////////

	protected final KeyMapper<K, V> keyMapper;

	/**
	 * creates a new key-value store manager
	 * 
	 * @param keyMapper
	 *            this optional parameter is suitable for situations where the key of an entry can be inferred from it directly
	 *            (alternatively, the key and value can be stored separately). when provided, several convenience methods become applicable
	 */
	public KeyValueStore(KeyMapper<K, V> keyMapper)
	{
		this.keyMapper = keyMapper;
	}

	// ////////////////////////////////////////////////////////////////////////////////////////////////

	/**
	 * returns a short-lived {@link KeyValueHandle} instance corresponding to a given key, or optionally a null if such key doesn't exist.
	 * The retrieved handle is used for a one-time CRUD operation on a key-value pair. Following this method, the returned handle will be
	 * requested for the entry itself (for either read or write purposes).
	 */
	protected abstract KeyValueHandle<K, V> getHandle(K key, Purpose purpose);

	/**
	 * naive implementation for retrieving multiple keys at once. subclasses may provide a better one.
	 */
	protected Iterator<KeyValueHandle<K, V>> getHandles(Set<K> keySet, Purpose purpose)
	{
		// TODO: we need a different approach here. the iterator needs to be used in updateMulti()
		List<KeyValueHandle<K, V>> list = new ArrayList<>();
		for (K key : keySet)
		{
			KeyValueHandle<K, V> kvh = getHandle(key, purpose);
			if (kvh != null)
				list.add(kvh);
		}
		return list.iterator();
	}

	/**
	 * As only one updater is desired at each given moment, whenever an updater thread starts the (non-atomic) process of updating a
	 * key-value, all other attempts (globally, of course, not just on a single machine) should be blocked
	 */
	protected abstract Lock getWriteLock(K key);

	public abstract Iterator<KeyValue<K, V>> iterator();

	public abstract Iterator<K> keyIterator();

	public abstract Iterator<V> entryIterator();

	public abstract <F> Index<K, V, F> createIndex(String indexName, IndexMapper<V, F> mapFunc);

	public abstract void truncate();

	/**
	 * insert a new entry, whose key doesn't already exist in storage. it's a faster way to insert entries compared to
	 * {@link #replace(Object, Object)} as it doesn't use any locking. do not use if you're not sure whether the key already exists. the
	 * behavior of the system in such case is undetermined and implementation-dependent.
	 */
	public abstract void insert(K key, V entry);

	// ////////////////////////////////////////////////////////////////////////////////////////////////

	public V get(K key)
	{
		KeyValueHandle<K, V> kvh = getHandle(key, Purpose.READ);
		if (kvh == null)
			return null;
		return kvh.getValue();
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	public Iterator<KeyValue<K, V>> iteratorFor(Set<K> keySet)
	{
		return (Iterator) getHandles(keySet, Purpose.READ);
	}

	public Iterator<V> entryIteratorFor(Set<K> keySet)
	{
		final Iterator<KeyValue<K, V>> iter = iteratorFor(keySet);
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

	/**
	 * naive implementation, using the key-iterator to create a set. subclass may have a better way.
	 */
	public Set<K> keySet()
	{
		return iterateInto(keyIterator(), new HashSet<K>());
	}

	/**
	 * naive implementation, using the iterator implementation to create a list. subclass may have a better way.
	 */
	public List<V> entries()
	{
		return iterateInto(entryIterator(), new ArrayList<V>());
	}

	public List<V> entriesFor(Set<K> keySet)
	{
		return iterateInto(entryIteratorFor(keySet), new ArrayList<V>());
	}

	// ////////////////////////////////////////////////////////////////////////////////////////////////

	/**
	 * inserts or updates an entry. if you're sure that the entry is new, use the more efficient {@link #insert(Object, Object)} instead
	 */
	public void replace(K key, V entry)
	{
		Lock lock = getWriteLock(key);
		lock.lock();
		try
		{
			KeyValueHandle<K, V> kvh = getHandle(key, Purpose.REPLACE);
			if (kvh == null)
				insert(key, entry);
			else
				kvh.commit(entry);
		}
		finally
		{
			lock.unlock();
		}
	}

	public V update(K key, Updater<V> updater)
	{
		KeyValueHandle<K, V> kvh = getHandle(key, Purpose.MODIFY);
		if (kvh == null)
		{
			updater.entryNotFound();
			return null;
		}

		Lock lock = getWriteLock(key);
		lock.lock();
		try
		{
			V entry = kvh.getModifiableValue();
			if (entry == null)
			{
				updater.entryNotFound();
				return null;
			}

			updater.changed = updater.update(entry);

			if (updater.changed)
				kvh.commit(entry);

			updater.postCommit(entry);

			return entry;
		}
		finally
		{
			lock.unlock();
		}
	}

	public void updateMultiple(Collection<K> keys, Updater<V> updater)
	{
		for (K key : keys)
		{
			update(key, updater);
			if (updater.stopped)
				break;
		}
	}

	/**
	 * convenience method
	 */
	public void updateAll(Updater<V> updater)
	{
		updateMultiple(keySet(), updater);
	}

	/**
	 * convenience method applicable when {@code keyMapper} is provided
	 * 
	 * @see {@link #update(Object, Updater)}
	 */
	public V updateEntry(V entry, Updater<V> updater)
	{
		return update(keyMapper.getKeyOf(entry), updater);
	}

	/**
	 * convenience method applicable when {@code keyMapper} is provided
	 * 
	 * @see {@link #insert(Object, Object)}
	 */
	public void insertEntry(V entry)
	{
		insert(keyMapper.getKeyOf(entry), entry);
	}

	/**
	 * convenience method applicable when {@code keyMapper} is provided
	 * 
	 * @see {@link #replace(Object, Object)}
	 */
	public void replaceEntry(V entry)
	{
		replace(keyMapper.getKeyOf(entry), entry);
	}

	// /////////////////////////////////////////////////////////////////////////////////////////////

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
