package com.tectonica.kvs;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import com.tectonica.kvs.KeyValueStore.KeyValue;

public abstract class AbstractIndex<K, V, F> implements Index<K, V, F>
{
	protected final IndexMapper<V, F> mapper;
	protected final String name;

	protected AbstractIndex(IndexMapper<V, F> mapper, String name)
	{
		this.mapper = mapper;

		if (name == null || name.isEmpty())
			throw new RuntimeException("index name is mandatory in " + AbstractIndex.class.getSimpleName());
		this.name = name;
	}

	// ///////////////////////////////////////////////////////////////////////////////////////

	@Override
	public abstract Iterator<KeyValue<K, V>> iteratorOf(F f);

	@Override
	public abstract Iterator<K> keyIteratorOf(F f);

	@Override
	public abstract Iterator<V> valueIteratorOf(F f);

	// ///////////////////////////////////////////////////////////////////////////////////////

	@Override
	public boolean containsKeyOf(F f)
	{
		return (keyIteratorOf(f).hasNext());
	}

	@Override
	public Set<K> keySetOf(F f)
	{
		return AbstractKeyValueStore.iterateInto(keyIteratorOf(f), new HashSet<K>());
	}

	@Override
	public List<V> valuesOf(F f)
	{
		return AbstractKeyValueStore.iterateInto(valueIteratorOf(f), new ArrayList<V>());
	}

	@Override
	public List<KeyValue<K, V>> entriesOf(F f)
	{
		return AbstractKeyValueStore.iterateInto(iteratorOf(f), new ArrayList<KeyValue<K, V>>());
	}

	@Override
	public Iterable<KeyValue<K, V>> asIterableOf(F f)
	{
		return AbstractKeyValueStore.iterableOf(iteratorOf(f));
	}

	@Override
	public Iterable<K> asKeyIterableOf(F f)
	{
		return AbstractKeyValueStore.iterableOf(keyIteratorOf(f));
	}

	@Override
	public Iterable<V> asValueIterableOf(F f)
	{
		return AbstractKeyValueStore.iterableOf(valueIteratorOf(f));
	}

	@Override
	public KeyValue<K, V> getFirstEntry(F f)
	{
		Iterator<KeyValue<K, V>> iter = iteratorOf(f);
		if (iter.hasNext())
			return iter.next();
		return null;
	}

	@Override
	public K getFirstKey(F f)
	{
		Iterator<K> iter = keyIteratorOf(f);
		if (iter.hasNext())
			return iter.next();
		return null;
	}

	@Override
	public V getFirstValue(F f)
	{
		Iterator<V> iter = valueIteratorOf(f);
		if (iter.hasNext())
			return iter.next();
		return null;
	}

	@Override
	public String getName()
	{
		return name;
	}
}