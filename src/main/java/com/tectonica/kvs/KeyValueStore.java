package com.tectonica.kvs;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import com.tectonica.kvs.Index.IndexMapper;
import com.tectonica.kvs.KeyValueStore.KeyValue;

/**
 * Simple, yet powerful, framework (and approach) for handling a key-value store. It allows for multiple concurrent readers but a single
 * concurrent writer on each entry. It also provides a read-before-update mechanism which makes life simpler and keeps data consistent.
 * Indexing is also supported as part of the framework. The interface is intuitive and straightforward. This class itself is abstract and
 * subclasses are included for several backend persistence engines, including in-memory (which is great for development).
 * <p>
 * The usage of this framework would probably yield the most benefit when used to prototype a data model, where changes are frequent and not
 * backwards-compatible. However, in more than a few scenarios, this framework can be used in production. The heavy-lifting is done by the
 * backend database and cache either way.
 * 
 * @author Zach Melamed
 */
public interface KeyValueStore<K, V> extends Iterable<KeyValue<K, V>>
{
	public static interface KeyValue<K, V>
	{
		K getKey();

		V getValue();
	}

	public static interface KeyMapper<K, V>
	{
		public K getKeyOf(V value);
	}

	public static interface ValueGenerator<K, V>
	{
		V generate(K key);
	}

	public static enum EventType
	{
		PreUpdate, PrePersistUpdate, PreAdd, PrePut;
	}

	public interface EventHandler<K, V>
	{
		public void handle(EventType type, K key, V value);
	}

	// GETTERS

	V get(K key);

	V get(K key, boolean cacheResult);

	/**
	 * Executes {@link #iteratorFor(Collection, boolean)} with {@code postponeCaching} set to false
	 */
	Iterator<KeyValue<K, V>> iteratorFor(Collection<K> keys);

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
	Iterator<KeyValue<K, V>> iteratorFor(Collection<K> keys, boolean postponeCaching);

	Iterator<K> keyIterator();

	Iterator<V> valueIterator();

	Iterator<V> valueIteratorFor(Collection<K> keys);

	Iterator<V> valueIteratorFor(Collection<K> keys, boolean postponeCaching);

	Set<K> keySet();

	List<V> values();

	List<V> valuesFor(Collection<K> keys);

	boolean containsKey(K key);

	// SETTERS

	/**
	 * inserts a new entry, whose key doesn't already exist in storage. it's a faster and more resource-efficient way to insert entries
	 * compared to {@link #put(Object, Object)} as it doesn't use any locking. do not use if you're not completely sure whether the key
	 * already exists. the behavior of the store in such case is undetermined and implementation-dependent.
	 */
	void add(K key, V value);

	/**
	 * inserts or updates an entry. if you're sure that the entry is new (i.e. its key doesn't already exist), use the more efficient
	 * {@link #add(Object, Object)} instead
	 */
	void put(K key, V value);

	/**
	 * insert and entry only if the key is not yet taken
	 * 
	 * @return
	 *         the newly added value, or null if nothing was inserted
	 */
	V putIfAbsent(K key, ValueGenerator<K, V> generator);

	V update(K key, Updater<V> updater);

	int update(Collection<K> keys, Updater<V> updater);

	/**
	 * convenience method to update all entries
	 */
	int updateAll(Updater<V> updater);

	/**
	 * convenience method applicable when {@code keyMapper} is provided
	 * 
	 * @see {@link #update(Object, Updater)}
	 */
	V updateValue(V value, Updater<V> updater);

	/**
	 * convenience method applicable when {@code keyMapper} is provided
	 * 
	 * @see {@link #add(Object, Object)}
	 */
	void addValue(V value);

	/**
	 * convenience method applicable when {@code keyMapper} is provided
	 * 
	 * @see {@link #put(Object, Object)}
	 */
	void putValue(V value);

	// DELETERS

	boolean delete(K key);

	int deleteAll();

	// ETC

	<F> Index<K, V, F> createIndex(String indexName, IndexMapper<V, F> mapFunc);

	void clearCache();

	void addListener(EventType type, EventHandler<K, V> handler);
}