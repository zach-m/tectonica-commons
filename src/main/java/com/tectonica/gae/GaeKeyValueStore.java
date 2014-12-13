package com.tectonica.gae;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
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

	private final Class<V> valueClass;
	private final String kind;
	private final Key ancestor; // dummy parent for all entities to guarantee Datastore consistency
	private final List<GaeIndexImpl<?>> indexes;

	public GaeKeyValueStore(Class<V> valueClass, KeyMapper<String, V> keyMapper)
	{
		super(keyMapper);
		if (valueClass == null)
			throw new NullPointerException();
		this.valueClass = valueClass;
		this.kind = valueClass.getSimpleName();
		this.ancestor = KeyFactory.createKey(kind, BOGUS_ANCESTOR_KEY_NAME);
		this.indexes = new ArrayList<>();
	}

	private Key keyOf(String key)
	{
		return KeyFactory.createKey(ancestor, kind, key);
	}

	/***********************************************************************************
	 * 
	 * GETTERS
	 * 
	 ***********************************************************************************/

	@Override
	protected V doGet(String key)
	{
		try
		{
			return entityToValue(ds.get(keyOf(key)));
		}
		catch (EntityNotFoundException e)
		{
			return null;
		}
	}

	@Override
	public Iterator<KeyValue<String, V>> iterator()
	{
		return entryIteratorOfQuery(newQuery()); // query without filters = all
	}

	@Override
	public Iterator<String> keyIterator()
	{
		return keyIteratorOfQuery(newQuery().setKeysOnly());
	}

	@Override
	public Iterator<V> valueIterator()
	{
		return valueIteratorOfQuery(newQuery());
	}

	@Override
	public Iterator<KeyValue<String, V>> iteratorFor(Collection<String> keySet)
	{
		if (keySet.isEmpty())
			return Collections.emptyIterator();

		List<Key> gaeKeySet = new ArrayList<>(keySet.size());
		for (String key : keySet)
			gaeKeySet.add(keyOf(key));

		Filter filter = new FilterPredicate(Entity.KEY_RESERVED_PROPERTY, FilterOperator.IN, gaeKeySet);
		return entryIteratorOfQuery(newQuery().setFilter(filter));
	}

	/***********************************************************************************
	 * 
	 * SETTERS (UTILS)
	 * 
	 ***********************************************************************************/

	@Override
	protected Modifier<String, V> getModifier(final String key, ModificationType purpose)
	{
		// insert, replace and update are all treated the same in GAE: all simply do save()
		return new Modifier<String, V>()
		{
			@Override
			public V getModifiableValue()
			{
				return doGet(key); // same implementation as read-only get, as in both cases we deserialize a new instance
			}

			@Override
			public void commit(V value)
			{
				save(key, value);
			}
		};
	}

	@Override
	public Lock getModificationLock(String key)
	{
		return GaeMemcacheLock.getLock(kind + ":" + key, true);
	}

	/***********************************************************************************
	 * 
	 * SETTERS
	 * 
	 ***********************************************************************************/

	@Override
	protected void doInsert(String key, V value)
	{
		save(key, value);
	}

	/***********************************************************************************
	 * 
	 * DELETERS
	 * 
	 ***********************************************************************************/

	@Override
	public void delete(String key)
	{
		ds.delete(keyOf(key));
	}

	@Override
	public void truncate()
	{
		for (Entity entity : ds.prepare(newQuery().setKeysOnly()).asIterable())
			ds.delete(entity.getKey());
	}

	/***********************************************************************************
	 * 
	 * INDEXES
	 * 
	 ***********************************************************************************/

	@Override
	public <F> Index<String, V, F> createIndex(String indexName, IndexMapper<V, F> mapFunc)
	{
		GaeIndexImpl<F> index = new GaeIndexImpl<>(mapFunc, indexName);
		indexes.add(index);
		return index;
	}

	/**
	 * GAE implementation of an index - simply exposes the Datastore property filters
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
		public Iterator<KeyValue<String, V>> iteratorOf(F f)
		{
			return entryIteratorOfQuery(newIndexQuery(f));
		}

		@Override
		public Iterator<String> keyIteratorOf(F f)
		{
			return keyIteratorOfQuery(newIndexQuery(f).setKeysOnly());
		}

		@Override
		public Iterator<V> valueIteratorOf(F f)
		{
			return valueIteratorOfQuery(newIndexQuery(f));
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

		private F getIndexedFieldOf(V value)
		{
			return mapper.getIndexedFieldOf(value);
		}
	}

	/***********************************************************************************
	 * 
	 * DATASTORE UTILS
	 * 
	 ***********************************************************************************/

	private static final String COL_NAME_ENTRY_VALUE = "value";
	private static final String COL_NAME_INDEX_PREFIX = "_i_";
	private static final String BOGUS_ANCESTOR_KEY_NAME = " ";

	private V entityToValue(Entity entity)
	{
		Blob blob = (Blob) entity.getProperty(COL_NAME_ENTRY_VALUE);
		return SerializeUtil.bytesToObj(blob.getBytes(), valueClass);
	}

	private Entity entryToEntity(String key, V value)
	{
		Entity entity = new Entity(kind, key, ancestor);
		entity.setProperty(COL_NAME_ENTRY_VALUE, new Blob(SerializeUtil.objToBytes(value)));
		for (int i = 0; i < indexes.size(); i++)
		{
			GaeIndexImpl<?> index = indexes.get(i);
			Object field = (value == null) ? null : index.getIndexedFieldOf(value);
			entity.setProperty(index.propertyName(), field);
		}
		return entity;
	}

	private void save(String key, V value)
	{
		ds.put(entryToEntity(key, value));
	}

	private Query newQuery()
	{
		return new Query(kind).setAncestor(ancestor);
	}

	private Iterator<KeyValue<String, V>> entryIteratorOfQuery(Query q)
	{
		final Iterator<Entity> iter = ds.prepare(q).asIterator();
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
						return entityToValue(entity);
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

	private Iterator<V> valueIteratorOfQuery(Query q)
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
				return entityToValue(iter.next());
			}

			@Override
			public void remove()
			{
				throw new UnsupportedOperationException();
			}
		};
	}
}
