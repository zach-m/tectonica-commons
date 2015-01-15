package com.tectonica.kvs;

import java.util.Iterator;
import java.util.List;
import java.util.Set;

import com.tectonica.kvs.KeyValueStore.KeyValue;

public interface Index<K, V, F>
{
	public static interface IndexMapper<V, F>
	{
		public F getIndexedFieldOf(V value);
	}

	Iterator<KeyValue<K, V>> iteratorOf(F f);

	Iterator<K> keyIteratorOf(F f);

	Iterator<V> valueIteratorOf(F f);

	boolean containsKeyOf(F f);

	Set<K> keySetOf(F f);

	List<V> valuesOf(F f);

	List<KeyValue<K, V>> entriesOf(F f);

	Iterable<KeyValue<K, V>> asIterableOf(F f);

	Iterable<K> asKeyIterableOf(F f);

	Iterable<V> asValueIterableOf(F f);

	KeyValue<K, V> getFirstEntry(F f);

	K getFirstKey(F f);

	V getFirstValue(F f);

	String getName();
}