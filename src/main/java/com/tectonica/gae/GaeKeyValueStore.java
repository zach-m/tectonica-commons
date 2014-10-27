package com.tectonica.gae;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
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
import com.tectonica.collections.KeyValueStore;
import com.tectonica.util.SerializeUtil;

public class GaeKeyValueStore<V extends Serializable> extends KeyValueStore<String, V>
{
	private static DatastoreService ds = DatastoreServiceFactory.getDatastoreService();

	private final Class<V> entryClass;
	private final String kind;

	// the Datastore guarantees strong consistency in ancestral queries. we create a bogus parent for all entities.
	// see: https://cloud.google.com/developers/articles/balancing-strong-and-eventual-consistency-with-google-cloud-datastore/
	private final Key ancestor;

	private static final String COL_NAME_ENTRY_VALUE = "value";
	private static final String COL_NAME_INDEX_PREFIX = "_i_";
	private static final String BOGUS_ANCESTOR_KEY_NAME = " ";

	private V entityToEntry(Entity entity)
	{
		Blob blob = (Blob) entity.getProperty(COL_NAME_ENTRY_VALUE);
		return SerializeUtil.bytesToObj(blob.getBytes(), entryClass);
	}

	private Entity entryToEntity(String key, V entry)
	{
		Entity entity = new Entity(kind, key, ancestor);
		entity.setProperty(COL_NAME_ENTRY_VALUE, new Blob(SerializeUtil.objToBytes(entry)));
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

	protected class GaeKeyValueHandler implements KeyValueHandle<String, V>
	{
		private final String _key; // is never null

		public GaeKeyValueHandler(String key)
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
		public V getValue()
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
		public V getModifiableValue()
		{
			return getValue(); // same implementation, as in both cases we deserialize a new instance
		}

		@Override
		public void commit(V entry)
		{
			ds.put(entryToEntity(_key, entry));
		}

		@Override
		public void delete()
		{
			ds.delete(KeyFactory.createKey(ancestor, kind, _key));
		}
	}

	private final List<GaeIndexImpl<?>> indices;

	public GaeKeyValueStore(Class<V> entryClass, KeyMapper<String, V> keyMapper)
	{
		super(keyMapper);
		if (entryClass == null)
			throw new NullPointerException();
		this.entryClass = entryClass;
		this.kind = entryClass.getSimpleName();
		this.ancestor = KeyFactory.createKey(kind, BOGUS_ANCESTOR_KEY_NAME);
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
		GaeKeyValueHandler kv = new GaeKeyValueHandler(key);
		kv.commit(entry);
	}

	@Override
	public void truncate()
	{
		for (Entity entity : ds.prepare(newQuery().setKeysOnly()).asIterable())
			ds.delete(entity.getKey());
	}

	@Override
	public Iterator<KeyValue<String, V>> iterator()
	{
		final Iterator<Entity> iter = ds.prepare(newQuery()).asIterator();
		return new Iterator<KeyValue<String, V>>()
		{
			@Override
			public boolean hasNext()
			{
				return iter.hasNext();
			}

			@Override
			public KeyValue<String, V> next()
			{
				final Entity entity = iter.next();
				return new KeyValue<String, V>()
				{
					@Override
					public String getKey()
					{
						return entity.getKey().getName();
					}

					@Override
					public V getValue()
					{
						return entityToEntry(entity);
					}
				};
			}

			@Override
			public void remove()
			{
				throw new UnsupportedOperationException();
			}
		};
	}

	@Override
	public Iterator<String> keyIterator()
	{
		return keyIteratorOfQuery(newQuery().setKeysOnly());
	}

	private Iterator<String> keyIteratorOfQuery(Query q)
	{
		final Iterator<Entity> iter = ds.prepare(q).asIterator();
		return new Iterator<String>()
		{
			@Override
			public boolean hasNext()
			{
				return iter.hasNext();
			}

			@Override
			public String next()
			{
				return iter.next().getKey().getName();
			}

			@Override
			public void remove()
			{
				throw new UnsupportedOperationException();
			}
		};
	}

	@Override
	public Iterator<V> entryIterator()
	{
		return entryIteratorOfQuery(newQuery());
	}

	private Iterator<V> entryIteratorOfQuery(Query q)
	{
		final Iterator<Entity> iter = ds.prepare(q).asIterator();
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
				return entityToEntry(iter.next());
			}

			@Override
			public void remove()
			{
				throw new UnsupportedOperationException();
			}
		};
	}

	@Override
	protected KeyValueHandle<String, V> getHandle(String key, Purpose purpose)
	{
		return new GaeKeyValueHandler(key);
	}

	@Override
	protected Iterator<KeyValueHandle<String, V>> getHandles(Set<String> keySet, Purpose purpose)
	{
		return super.getHandles(keySet, purpose); // TODO: in READ scenarios, we can pre-fetch all keys in a single roundtrip
	}

	@Override
	public Lock getWriteLock(String key)
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

		public Iterator<String> keyIteratorOf(F f)
		{
			return keyIteratorOfQuery(newIndexQuery(f).setKeysOnly());
		}

		@Override
		public Iterator<V> entryIteratorOf(F f)
		{
			return entryIteratorOfQuery(newIndexQuery(f));
		}

		private Query newIndexQuery(F f)
		{
			Filter filter = new FilterPredicate(propertyName(), FilterOperator.EQUAL, f);
			return newQuery().setFilter(filter);
		}

		private String propertyName()
		{
			return COL_NAME_INDEX_PREFIX + name;
		}

		private F getIndexedFieldOf(V entry)
		{
			return mapFunc.getIndexedFieldOf(entry);
		}
	}
}
