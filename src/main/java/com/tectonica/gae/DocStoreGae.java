package com.tectonica.gae;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.locks.Lock;

import com.google.appengine.api.datastore.Blob;
import com.google.appengine.api.datastore.DatastoreService;
import com.google.appengine.api.datastore.DatastoreServiceFactory;
import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.EntityNotFoundException;
import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.KeyFactory;
import com.google.appengine.api.datastore.Query;
import com.google.appengine.api.datastore.Query.Filter;
import com.google.appengine.api.datastore.Query.FilterOperator;
import com.google.appengine.api.datastore.Query.FilterPredicate;
import com.tectonica.collections.DocStore;
import com.tectonica.util.SerializeUtil;

public class DocStoreGae<V extends Serializable> extends DocStore<String, V>
{
	private static DatastoreService ds = DatastoreServiceFactory.getDatastoreService();

	private final Class<V> entryClass;
	private final String kind;

	// the Datastore guarantees strong consistency in ancestral queries. we create a bogus parent for all entities.
	// see: https://cloud.google.com/developers/articles/balancing-strong-and-eventual-consistency-with-google-cloud-datastore/
	private final Key ancestor;

	private static final String VALUE_PROPERTY_NAME = "value";

	private V entityToEntry(Entity entity)
	{
		Blob blob = (Blob) entity.getProperty(VALUE_PROPERTY_NAME);
		return SerializeUtil.bytesToObj(blob.getBytes(), entryClass);
	}

	private Entity entryToEntity(String key, V entry)
	{
		Entity entity = new Entity(kind, key, ancestor);
		entity.setProperty(VALUE_PROPERTY_NAME, new Blob(SerializeUtil.objToBytes(entry)));
		for (int i = 0; i < indices.size(); i++)
		{
			GaeIndexImpl<?> index = indices.get(i);
			Object field = (entry == null) ? null : index.getIndexedFieldOf(entry);
			entity.setProperty(index.propertyName(), field);
		}
		return entity;
	}

	private Query newQuery()
	{
		return new Query(kind).setAncestor(ancestor);
	}

	protected class GaeDocument implements Document<String, V>
	{
		private final String _key; // is never null

		public GaeDocument(String key)
		{
			if (key == null)
				throw new NullPointerException();
			_key = key;
		}

		@Override
		public String getKey()
		{
			return _key;
		}

		@Override
		public V get()
		{
			Entity entity;
			try
			{
				// lookup-by-key is always strongly consistent in the Datastore
				entity = ds.get(KeyFactory.createKey(ancestor, kind, _key));
				return entityToEntry(entity);
			}
			catch (EntityNotFoundException e)
			{
				return null;
			}
		}

		@Override
		public V getForWrite()
		{
			return get(); // same implementation, as in both cases we deserialize a new instance
		}

		@Override
		public void commit(V entry)
		{
			ds.put(entryToEntity(_key, entry));
		}
	}

	private final List<GaeIndexImpl<?>> indices;

	public DocStoreGae(Class<V> entryClass, KeyMapper<String, V> keyMapper)
	{
		super(keyMapper);
		if (entryClass == null)
			throw new NullPointerException();
		this.entryClass = entryClass;
		this.kind = entryClass.getSimpleName();
		this.ancestor = KeyFactory.createKey(kind, " ");
		this.indices = new ArrayList<>();
	}

	@Override
	public <F> Index<String, V, F> createIndex(String indexName, IndexMapper<V, F> mapFunc)
	{
		GaeIndexImpl<F> index = new GaeIndexImpl<>(mapFunc, indexName);
		indices.add(index);
		return index;
	}

	@Override
	public void insert(String key, V entry)
	{
		GaeDocument doc = new GaeDocument(key);
		doc.commit(entry);
	}

	@Override
	public void truncate()
	{
		for (Entity entity : ds.prepare(newQuery().setKeysOnly()).asIterable())
			ds.delete(entity.getKey());
	}

	@Override
	protected Set<String> getAllKeys()
	{
		return keysOfQuery(newQuery().setKeysOnly());
	}

	private Set<String> keysOfQuery(Query q)
	{
		Set<String> keySet = new HashSet<>();
		for (Entity entity : ds.prepare(q).asIterable())
			keySet.add(entity.getKey().getName());
		return keySet;
	}

	@Override
	protected Document<String, V> getDocument(String key, DocumentPurpose purpose)
	{
		return new GaeDocument(key);
	}

	@Override
	public Lock lockForWrite(String key)
	{
		return GaeMemcacheLock.getLock(kind + ":" + key, true);
	}

	/**
	 * straightforward in-memory implementation of an index
	 * 
	 * @author Zach Melamed
	 */
	public class GaeIndexImpl<F> extends Index<String, V, F>
	{
		public GaeIndexImpl(IndexMapper<V, F> mapFunc, String name)
		{
			super(mapFunc, name);

			if (name == null || name.isEmpty())
				throw new RuntimeException("index name is mandatory in " + GaeIndexImpl.class.getSimpleName());
		}

		@Override
		public Set<String> getKeysOf(F f)
		{
			Filter filter = new FilterPredicate(propertyName(), FilterOperator.EQUAL, f);
			return keysOfQuery(newQuery().setFilter(filter).setKeysOnly());
		}

		@Override
		public Collection<V> getEntriesOf(F f)
		{
			Collection<V> entries = new ArrayList<>();
			Filter filter = new FilterPredicate(propertyName(), FilterOperator.EQUAL, f);
			for (Entity entity : ds.prepare(newQuery().setFilter(filter)).asIterable())
				entries.add(entityToEntry(entity));
			return entries;
		}

		private String propertyName()
		{
			return "_i_" + name;
		}

		private F getIndexedFieldOf(V entry)
		{
			return mapFunc.getIndexedFieldOf(entry);
		}
	}
}
