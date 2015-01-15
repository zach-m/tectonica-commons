package com.tectonica.kvs;

import java.util.ArrayList;
import java.util.HashSet;
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

	@Override
	public String getName()
	{
		return name;
	}

	// ///////////////////////////////////////////////////////////////////////////////////////

	@Override
	public boolean containsKeyOf(F f)
	{
		return (keyIteratorOf(f).hasNext());
	}

	@Override
	public Set<K> keySetOf(F f)
	{
		return KvsUtil.iterateInto(keyIteratorOf(f), new HashSet<K>());
	}

	@Override
	public List<V> valuesOf(F f)
	{
		return KvsUtil.iterateInto(valueIteratorOf(f), new ArrayList<V>());
	}

	@Override
	public List<KeyValue<K, V>> entriesOf(F f)
	{
		return KvsUtil.iterateInto(iteratorOf(f), new ArrayList<KeyValue<K, V>>());
	}

	@Override
	public Iterable<KeyValue<K, V>> asIterableOf(F f)
	{
		return KvsUtil.iterableOf(iteratorOf(f));
	}

	@Override
	public Iterable<K> asKeyIterableOf(F f)
	{
		return KvsUtil.iterableOf(keyIteratorOf(f));
	}

	@Override
	public Iterable<V> asValueIterableOf(F f)
	{
		return KvsUtil.iterableOf(valueIteratorOf(f));
	}

	@Override
	public KeyValue<K, V> getFirstEntry(F f)
	{
		return KvsUtil.firstOf(iteratorOf(f));
	}

	@Override
	public K getFirstKey(F f)
	{
		return KvsUtil.firstOf(keyIteratorOf(f));
	}

	@Override
	public V getFirstValue(F f)
	{
		return KvsUtil.firstOf(valueIteratorOf(f));
	}
}