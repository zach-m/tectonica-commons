package com.tectonica.gae;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
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
import com.google.appengine.api.memcache.MemcacheService;
import com.google.appengine.api.memcache.MemcacheServiceFactory;
import com.tectonica.collections.KeyValueStore;
import com.tectonica.util.KryoUtil;
import com.tectonica.util.SerializeUtil;

public class GaeKeyValueStore<V extends Serializable> extends KeyValueStore<String, V>
{
	private static DatastoreService ds = DatastoreServiceFactory.getDatastoreService();

	private final Class<V> valueClass;
	private final String kind;
	private final Key ancestor; // dummy parent for all entities to guarantee Datastore consistency
	private final Serializer<V> serializer;
	private final List<GaeIndexImpl<?>> indexes;

	public GaeKeyValueStore(Class<V> valueClass, KeyMapper<String, V> keyMapper)
	{
		this(valueClass, keyMapper, new JavaSerializer<V>());
	}

	public GaeKeyValueStore(Class<V> valueClass, KeyMapper<String, V> keyMapper, Serializer<V> serializer)
	{
		super(keyMapper);
		if (valueClass == null)
			throw new NullPointerException("valueClass");
		if (serializer == null)
			throw new NullPointerException("serializer");
		this.valueClass = valueClass;
		this.kind = valueClass.getSimpleName();
		this.ancestor = KeyFactory.createKey(kind, BOGUS_ANCESTOR_KEY_NAME);
		this.indexes = new ArrayList<>();
		this.serializer = serializer;
	}

	private Key keyOf(String key)
	{
		return KeyFactory.createKey(ancestor, kind, key);
	}

	@Override
	protected Cache<String, V> createCache()
	{
		if (serializer instanceof JavaSerializer)
			return new JavaSerializeCache();
		return new CustomSerializeCache();
	}

	/***********************************************************************************
	 * 
	 * GETTERS
	 * 
	 ***********************************************************************************/

	@Override
	protected V dbRead(String key)
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
	protected Iterator<KeyValue<String, V>> dbOrderedIterator(Collection<String> keys)
	{
		if (keys.size() > 30)
			throw new RuntimeException("GAE doesn't support more than 30 at the time, need to break it");

		List<Key> gaeKeys = new ArrayList<>(keys.size());
		for (String key : keys)
			gaeKeys.add(keyOf(key));

		// we define a filter based on the IN operator, which returns values in the order of listing.
		// see: https://cloud.google.com/appengine/docs/java/datastore/queries#Java_Query_structure
		Filter filter = new FilterPredicate(Entity.KEY_RESERVED_PROPERTY, FilterOperator.IN, gaeKeys);
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
				// we use here same calls as if for read-only value, because in both cases a new instance is deserialized
				// NOTE: if we ever switch to a different implementation, with local objects, this wouldn't work
				return get(key, false);
			}

			@Override
			public void dbWrite(V value)
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
	protected void dbInsert(String key, V value)
	{
		save(key, value);
	}

	/***********************************************************************************
	 * 
	 * DELETERS
	 * 
	 ***********************************************************************************/

	@Override
	protected boolean dbDelete(String key)
	{
		ds.delete(keyOf(key));
		return true; // we don't really know if the key previously existed
	}

	@Override
	protected int dbDeleteAll()
	{
		int removed = 0;
		for (Entity entity : ds.prepare(newQuery().setKeysOnly()).asIterable())
		{
			ds.delete(entity.getKey());
			removed++;
		}
		return removed; // an estimate. we have to assume that all keys existed before delete
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
	private class GaeIndexImpl<F> extends Index<String, V, F>
	{
		public GaeIndexImpl(IndexMapper<V, F> mapFunc, String name)
		{
			super(mapFunc, name);
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
		return serializer.bytesToObj(blob.getBytes(), valueClass);
	}

	private Entity entryToEntity(String key, V value)
	{
		Entity entity = new Entity(kind, key, ancestor);
		entity.setProperty(COL_NAME_ENTRY_VALUE, new Blob(serializer.objToBytes(value)));
		for (GaeIndexImpl<?> index : indexes)
		{
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

	/***********************************************************************************
	 * 
	 * SERIALIZATION
	 * 
	 ***********************************************************************************/

	public static interface Serializer<V>
	{
		V bytesToObj(byte[] bytes, Class<V> clz); // NOTE: if bytes is null, return null

		byte[] objToBytes(V obj); // NOTE: if obj is null, return null
	}

	private static final class JavaSerializer<V> implements Serializer<V>
	{
		@Override
		public V bytesToObj(byte[] bytes, Class<V> clz)
		{
			return SerializeUtil.bytesToObj(bytes, clz);
		}

		@Override
		public byte[] objToBytes(V obj)
		{
			return SerializeUtil.objToBytes(obj);
		}
	}

	public static class KryoSerializer<V> implements Serializer<V>
	{
		@Override
		public V bytesToObj(byte[] bytes, Class<V> clz)
		{
			return KryoUtil.bytesToObj(bytes, clz);
		}

		@Override
		public byte[] objToBytes(V obj)
		{
			return KryoUtil.objToBytes(obj);
		}
	}

	/***********************************************************************************
	 * 
	 * CACHE IMPLEMENTATION
	 * 
	 ***********************************************************************************/

	private class JavaSerializeCache implements Cache<String, V>
	{
		private MemcacheService mc = MemcacheServiceFactory.getMemcacheService(kind);

		@Override
		@SuppressWarnings("unchecked")
		public V get(String key)
		{
			return (V) mc.get(key);
		}

		@Override
		@SuppressWarnings("unchecked")
		public Map<String, V> get(Collection<String> keys)
		{
			return (Map<String, V>) (Map<String, ?>) mc.getAll(keys);
		}

		@Override
		public void put(String key, V value)
		{
			mc.put(key, value);
		}

		@Override
		public void put(Map<String, V> values)
		{
			mc.putAll(values);
		}

		@Override
		public void delete(String key)
		{
			mc.delete(key);
		}

		@Override
		public void deleteAll()
		{
			mc.clearAll();
		}
	};

	private class CustomSerializeCache implements Cache<String, V>
	{
		private MemcacheService mc = MemcacheServiceFactory.getMemcacheService(kind);

		@Override
		public V get(String key)
		{
			byte[] bytes = (byte[]) mc.get(key);
			return serializer.bytesToObj(bytes, valueClass);
		}

		@Override
		@SuppressWarnings("unchecked")
		public Map<String, V> get(Collection<String> keys)
		{
			Map<String, Object> values = mc.getAll(keys);
			Iterator<Entry<String, Object>> iter = values.entrySet().iterator();
			while (iter.hasNext())
			{
				Entry<String, Object> entry = iter.next();
				byte[] bytes = (byte[]) entry.getValue();
				entry.setValue(serializer.bytesToObj(bytes, valueClass));
			}
			return (Map<String, V>) (Map<String, ?>) values;
		}

		@Override
		public void put(String key, V value)
		{
			mc.put(key, serializer.objToBytes(value));
		}

		@Override
		@SuppressWarnings("unchecked")
		public void put(Map<String, V> values)
		{
			// NOTE: we make a huge assumption here, that we can modify the passed map. by doing so we rely on "inside information" that
			// this is harmless given how 'iteratorFor' is implemented
			Iterator<Entry<String, Object>> iter = ((Map<String, Object>) (Map<String, ?>) values).entrySet().iterator();
			while (iter.hasNext())
			{
				Entry<String, Object> entry = iter.next();
				Object value = entry.getValue();
				entry.setValue(serializer.objToBytes((V) value));
			}
			mc.putAll(values);
		}

		@Override
		public void delete(String key)
		{
			mc.delete(key);
		}

		@Override
		public void deleteAll()
		{
			mc.clearAll();
		}
	};
}
