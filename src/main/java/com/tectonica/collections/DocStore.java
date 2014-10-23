package com.tectonica.collections;

import java.util.Collection;
import java.util.Set;
import java.util.concurrent.locks.Lock;

/**
 * Simple, yet powerful, framework (and approach) for handling a key-value store. It allows for multiple concurrent readers yet a single
 * concurrent writer of each entry. It also provides a read-before-update mechanism which makes life simpler and data more consistent.
 * Indexing is also supported as part of the framework. The interface is intuitive and straitforward.The class itself is abstract and
 * subclasses may be created for many backend persistence engines, including in-memory.
 * <p>
 * The usage of this framework would probably yield the most benefit when used to prototype a data model, where changes are frequent and not
 * backwards-compatible. However, there is absolutely no reason to not use this framework in production. The heavy-lifting is done by the
 * backend database either way.
 * 
 * @author Zach Melamed
 */
public abstract class DocStore<K, V>
{
	public interface Document<K, V>
	{
		K getKey();

		/**
		 * returns the actual data of the document
		 */
		V get();

		/**
		 * Returns an instance that can be safely modified by the caller. During this modification, calls to {@link #get()} will return the
		 * unchanged value. If the instance was indeed modified by the caller, and no exception occurred in the process,
		 * {@link #commit(Object)} will be invoked.
		 * <p>
		 * NOTE: this method is called only on a locked document
		 */
		V getForWrite();

		/**
		 * Makes the changes to a document permanent. After this method finishes, calls to {@link #get()} will return the updated document.
		 * <p>
		 * NOTE: this method is called only on a locked document
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

		abstract public Set<K> getKeysOf(F f);

		abstract public Collection<V> getEntriesOf(F f);

		public K getFirstKey(F f)
		{
			Set<K> set = getKeysOf(f);
			if (set == null || set.size() == 0)
				return null;
			return set.iterator().next();
		}

		public V getFirstEntry(F f)
		{
			Collection<V> list = getEntriesOf(f);
			if (list == null || list.size() == 0)
				return null;
			return list.iterator().next();
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
		 * method inside which an entry may be safely modified
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
		 * called if an update process was requested for a non existing key
		 */
		public void entryNotFound()
		{}
	}

	public static enum DocumentPurpose
	{
		READ, MODIFY, REPLACE;
	}

	// ////////////////////////////////////////////////////////////////////////////////////////////////

	protected final KeyMapper<K, V> keyMapper;

	/**
	 * creates a new document store manager
	 * 
	 * @param keyMapper
	 *            this optional parameter is suitable for situations where the key of an entry can be inferred from it directly
	 *            (alternatively, the key and value can be stored separately). when provided, several convenience methods become applicable
	 */
	public DocStore(KeyMapper<K, V> keyMapper)
	{
		this.keyMapper = keyMapper;
	}

	protected abstract Set<K> getAllKeys(); // TODO: maybe needs to be public?

	/**
	 * returns a short-lived {@link Document} instance corresponding to the given key, or null if such key doesn't exist. The retrieved
	 * document is used for a one-time CRUD operation on a key-value pair. Following this method, the returned document will be requested
	 * for the entry itself (for either read or write purposes).
	 */
	protected abstract Document<K, V> getDocument(K key, DocumentPurpose purpose);

	/**
	 * As only one updater is desired at each given moment, whenever an updater thread starts the (non-atomic) process of updating a
	 * document, all other attempts (globally, of course, not just on a single machine) should be blocked
	 */
	protected abstract Lock lockForWrite(K key);

	// ////////////////////////////////////////////////////////////////////////////////////////////////

	public abstract <F> Index<K, V, F> createIndex(String indexName, IndexMapper<V, F> mapFunc);

	public abstract void truncate();

	/**
	 * insert a new entry, whose key doesn't already exist in storage. it's a faster way to insert entries compared to
	 * {@link #replace(Object, Object)} as it doesn't use any locking. do not use if you're not sure whether the key already exists. the
	 * behavior of the system in such case is undetermined and implementation-dependent.
	 */
	public abstract void insert(K key, V entry);

	/**
	 * inserts or updates an entry. if you're sure that the entry is new, use the more efficient {@link #insert(Object, Object)} instead
	 */
	public void replace(K key, V entry)
	{
		Lock lock = lockForWrite(key);
		lock.lock();
		try
		{
			Document<K, V> doc = getDocument(key, DocumentPurpose.REPLACE);
			V existing = (doc == null) ? null : doc.get();

			if (existing == null)
				insert(key, entry);
			else
				doc.commit(entry);
		}
		finally
		{
			lock.unlock();
		}
	}

	public V get(K key)
	{
		Document<K, V> doc = getDocument(key, DocumentPurpose.READ);
		if (doc == null)
			return null;
		return doc.get();
	}

	public V update(K key, Updater<V> updater)
	{
		Document<K, V> doc = getDocument(key, DocumentPurpose.MODIFY);
		if (doc == null)
		{
			updater.entryNotFound();
			return null;
		}

		Lock lock = lockForWrite(key);
		lock.lock();
		try
		{
			V entry = doc.getForWrite();
			if (entry == null)
			{
				updater.entryNotFound();
				return null;
			}

			updater.changed = updater.update(entry);

			if (updater.changed)
				doc.commit(entry);

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
		updateMultiple(getAllKeys(), updater);
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
}
