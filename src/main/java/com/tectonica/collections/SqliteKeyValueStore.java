/*
 * Copyright (C) 2014 Zach Melamed
 * 
 * Latest version available online at https://github.com/zach-m/tectonica-commons
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.tectonica.collections;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.tectonica.core.STR;
import com.tectonica.jdbc.JDBC;
import com.tectonica.jdbc.SqliteUtil;
import com.tectonica.jdbc.JDBC.ConnListener;
import com.tectonica.jdbc.JDBC.ExecutionContext;
import com.tectonica.jdbc.ResultSetIterator;
import com.tectonica.util.SerializeUtil;

public class SqliteKeyValueStore<V extends Serializable> extends KeyValueStore<String, V>
{
	private final Class<V> valueClass;
	private final String table;
	private final Serializer<V> serializer;
	private final List<SqliteIndexImpl<?>> indexes;
	private final List<String> indexeCols;
	private final JDBC jdbc;

	/**
	 * creates a new key-value store backed by Sqlite
	 * 
	 * @param keyMapper
	 *            this optional parameter is suitable in situations where the key of an entry can be inferred from its value directly
	 *            (as opposed to when the key and value are stored separately). when provided, several convenience methods become applicable
	 */
	public SqliteKeyValueStore(Class<V> valueClass, String connStr, KeyMapper<String, V> keyMapper)
	{
		super(keyMapper);
		this.valueClass = valueClass;
		this.table = valueClass.getSimpleName();
		this.serializer = new JavaSerializer<V>();
		this.indexes = new ArrayList<>();
		this.indexeCols = new ArrayList<>();
		this.jdbc = SqliteUtil.connect(connStr);
		createTable();
	}

	@Override
	protected Cache<String, V> createCache()
	{
		return new InMemCache();
	}

	/***********************************************************************************
	 * 
	 * GETTERS
	 * 
	 ***********************************************************************************/

	@Override
	protected V dbGet(final String key)
	{
		return jdbc.execute(new ConnListener<V>()
		{
			@Override
			public V onConnection(Connection conn) throws SQLException
			{
				PreparedStatement stmt = conn.prepareStatement(sqlSelectSingle());
				stmt.setString(1, key);
				ResultSet rs = stmt.executeQuery();
				byte[] bytes = (rs.next()) ? rs.getBytes(1) : null;
				return serializer.bytesToObj(bytes, valueClass);
			}
		});
	}

	@Override
	public Iterator<KeyValue<String, V>> iterator()
	{
		ExecutionContext ctx = jdbc.startExecute(new ConnListener<ResultSet>()
		{
			@Override
			public ResultSet onConnection(Connection conn) throws SQLException
			{
				return conn.createStatement().executeQuery(sqlSelectAll());
			}
		});
		return entryIteratorOfResultSet(ctx);
	}

	@Override
	protected Iterator<KeyValue<String, V>> dbOrderedIterator(final Collection<String> keys)
	{
		return jdbc.execute(new ConnListener<Iterator<KeyValue<String, V>>>()
		{
			@Override
			public Iterator<KeyValue<String, V>> onConnection(Connection conn) throws SQLException
			{
				PreparedStatement stmt = conn.prepareStatement(sqlSelectKeys(keys));
				int i = 0;
				for (String key : keys)
					stmt.setString(++i, key);

				// fetch all results into memory
				Map<String, byte[]> prefetch = new HashMap<>();
				ResultSet rs = stmt.executeQuery();
				while (rs.next())
					prefetch.put(rs.getString(1), rs.getBytes(2));

				// sort by the same order of the keys passed by the user
				List<KeyValue<String, byte[]>> ordered = orderByKeys(prefetch, keys);
				return entryIteratorOfBytesIter(ordered.iterator());
			}
		});
	}

	@Override
	public Iterator<String> keyIterator()
	{
		ExecutionContext ctx = jdbc.startExecute(new ConnListener<ResultSet>()
		{
			@Override
			public ResultSet onConnection(Connection conn) throws SQLException
			{
				return conn.createStatement().executeQuery(sqlSelectAll());
			}
		});
		return keyIteratorOfResultSet(ctx);
	}

	@Override
	public Iterator<V> valueIterator()
	{
		ExecutionContext ctx = jdbc.startExecute(new ConnListener<ResultSet>()
		{
			@Override
			public ResultSet onConnection(Connection conn) throws SQLException
			{
				return conn.createStatement().executeQuery(sqlSelectAll());
			}
		});
		return valueIteratorOfResultSet(ctx);
	}

	@Override
	public boolean containsKey(final String key)
	{
		return jdbc.execute(new ConnListener<Boolean>()
		{
			@Override
			public Boolean onConnection(Connection conn) throws SQLException
			{
				PreparedStatement stmt = conn.prepareStatement(sqlSelectContainKey(key));
				stmt.setString(1, key);
				return stmt.executeQuery().next();
			}
		});
	}

	/***********************************************************************************
	 * 
	 * SETTERS (UTILS)
	 * 
	 ***********************************************************************************/

	@Override
	protected Modifier<String, V> getModifier(final String key, ModificationType purpose)
	{
		return new Modifier<String, V>()
		{
			@Override
			public V getModifiableValue()
			{
				V value = usingCache ? cache.get(key) : null;
				if (value != null) // if we get a (local, in-memory) copy from the cache, we have to return a duplicate
					return serializer.copyOf(value);
				return dbGet(key);
			}

			@Override
			public void dbPut(final V value)
			{
				int updated = upsertRow(key, value, false);
				if (updated != 1)
					throw new RuntimeException("Unexpected dbUpdate() count: " + updated);
			}
		};
	}

	private AutoEvictMap<String, Lock> locks = new AutoEvictMap<String, Lock>(new AutoEvictMap.Factory<String, Lock>()
	{
		@Override
		public Lock valueOf(final String key)
		{
			return new ReentrantLock()
			{
				private static final long serialVersionUID = 1L;

				@Override
				public void unlock()
				{
					super.unlock();
					locks.release(key);
				}
			};
		}
	});

	@Override
	protected Lock getModificationLock(String key)
	{
		try
		{
			return locks.acquire(key);
		}
		catch (InterruptedException e)
		{
			throw new RuntimeException(e);
		}
	}

	/***********************************************************************************
	 * 
	 * SETTERS
	 * 
	 ***********************************************************************************/

	@Override
	protected void dbInsert(final String key, final V value)
	{
		int inserted = upsertRow(key, value, true);
		if (inserted != 1)
			throw new RuntimeException("Unexpected dbInsert() count: " + inserted);
	}

	/***********************************************************************************
	 * 
	 * DELETERS
	 * 
	 ***********************************************************************************/

	@Override
	protected boolean dbDelete(final String key)
	{
		int deleted = jdbc.execute(new ConnListener<Integer>()
		{
			@Override
			public Integer onConnection(Connection conn) throws SQLException
			{
				PreparedStatement stmt = conn.prepareStatement(sqlDeleteSingle());
				stmt.setString(1, key);
				return stmt.executeUpdate();
			}
		});
		return (deleted != 0);
	}

	@Override
	protected int dbDeleteAll()
	{
		return jdbc.execute(new ConnListener<Integer>()
		{
			@Override
			public Integer onConnection(Connection conn) throws SQLException
			{
				return conn.createStatement().executeUpdate(sqlDeleteAll());
			}
		});
	}

	/***********************************************************************************
	 * 
	 * INDEXES
	 * 
	 ***********************************************************************************/

	@Override
	public <F> Index<String, V, F> createIndex(final String indexName, IndexMapper<V, F> mapFunc)
	{
		jdbc.execute(new ConnListener<Void>()
		{
			@Override
			public Void onConnection(Connection conn) throws SQLException
			{
				try
				{
					conn.createStatement().executeUpdate(sqlAddColumn(indexName));
				}
				catch (Exception e)
				{
					// probably 'duplicate column name'
					System.out.println(e.toString());
				}
				conn.createStatement().executeUpdate(sqlCreateIndex(indexName));
				return null;
			}
		});
		SqliteIndexImpl<F> index = new SqliteIndexImpl<>(mapFunc, indexName);
		indexes.add(index);
		indexeCols.add(colOfIndex(indexName));
		return index;
	}

	private class SqliteIndexImpl<F> extends Index<String, V, F>
	{
		public SqliteIndexImpl(IndexMapper<V, F> mapFunc, String name)
		{
			super(mapFunc, name);
		}

		@Override
		public Iterator<KeyValue<String, V>> iteratorOf(final F f)
		{
			return entryIteratorOfResultSet(selectByIndex(f));
		}

		@Override
		public Iterator<String> keyIteratorOf(F f)
		{
			return keyIteratorOfResultSet(selectByIndex(f));
		}

		@Override
		public Iterator<V> valueIteratorOf(F f)
		{
			return valueIteratorOfResultSet(selectByIndex(f));
		}

		private ExecutionContext selectByIndex(final F f)
		{
			ExecutionContext ctx = jdbc.startExecute(new ConnListener<ResultSet>()
			{
				@Override
				public ResultSet onConnection(Connection conn) throws SQLException
				{
					PreparedStatement stmt = conn.prepareStatement(sqlSelectByIndex(name));
					stmt.setString(1, f.toString());
					return stmt.executeQuery();
				}
			});
			return ctx;
		}

		private String getIndexedFieldOf(V value)
		{
			F idx = mapper.getIndexedFieldOf(value);
			return (idx == null) ? null : idx.toString();
		}
	}

	/***********************************************************************************
	 * 
	 * SQL QUERIES
	 * 
	 ***********************************************************************************/

	private String sqlCreateTable()
	{
		return String.format("CREATE TABLE IF NOT EXISTS %s (K VARCHAR2 PRIMARY KEY, V BLOB)", table);
	}

	private String sqlSelectSingle()
	{
		return String.format("SELECT V FROM %s WHERE K=?", table);
	}

	private String sqlSelectAll()
	{
		return String.format("SELECT K,V FROM %s", table);
	}

	private String sqlSelectKeys(Collection<String> keys)
	{
		return String.format("SELECT K,V FROM %s WHERE K IN (%s)", table, STR.implode("?", ",", keys.size()));
	}

	private String sqlSelectContainKey(String key)
	{
		return String.format("SELECT 1 FROM %s WHERE K=?", table);
	}

	private String sqlUpsert(boolean strictInsert)
	{
		String statement = strictInsert ? "INSERT" : "REPLACE";
		String pfx = (indexeCols.size() > 0) ? "," : "";
		String cols = pfx + STR.implode(indexeCols, ",", false);
		String qm = pfx + STR.implode("?", ",", indexeCols.size());
		return String.format("%s INTO %s (K,V %s) VALUES (?,? %s)", statement, table, cols, qm);
	}

	private String sqlDeleteSingle()
	{
		return String.format("DELETE FROM %s WHERE K=?", table);
	}

	private String sqlDeleteAll()
	{
		return String.format("DELETE FROM %s", table); // not TRUNCATE, as we want the deleted-count
	}

	private String sqlAddColumn(String indexName)
	{
		// TODO: we save the indexes values as strings instead of their possibly other native data type, can be improved
		return String.format("ALTER TABLE %s ADD COLUMN %s VARCHAR2", table, colOfIndex(indexName));
	}

	private String sqlCreateIndex(String indexName)
	{
		return String.format("CREATE INDEX IF NOT EXISTS IDX_%s ON %s (%s)", indexName, table, colOfIndex(indexName));
	}

	private String sqlSelectByIndex(String indexName)
	{
		return String.format("SELECT K,V FROM %s WHERE %s=?", table, colOfIndex(indexName));
	}

	private String colOfIndex(String indexName)
	{
		return "_i_" + indexName;
	}

	/***********************************************************************************
	 * 
	 * DATABASE UTILS
	 * 
	 ***********************************************************************************/

	private void createTable()
	{
		jdbc.execute(new ConnListener<Void>()
		{
			@Override
			public Void onConnection(Connection conn) throws SQLException
			{
				conn.createStatement().execute(sqlCreateTable());
				return null;
			}
		});
	}

	private Iterator<KeyValue<String, V>> entryIteratorOfResultSet(final ExecutionContext ctx)
	{
		final ResultSetIterator iter = new ResultSetIterator(ctx);
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
				final ResultSet rs = iter.next();
				return new KeyValue<String, V>()
				{
					@Override
					public String getKey()
					{
						return rsGetKey(rs);
					}

					@Override
					public V getValue()
					{
						return rsGetValue(rs);
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

	private Iterator<String> keyIteratorOfResultSet(final ExecutionContext ctx)
	{
		final ResultSetIterator iter = new ResultSetIterator(ctx);
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
				return rsGetKey(iter.next());
			}

			@Override
			public void remove()
			{
				throw new UnsupportedOperationException();
			}
		};
	}

	private Iterator<V> valueIteratorOfResultSet(final ExecutionContext ctx)
	{
		final ResultSetIterator iter = new ResultSetIterator(ctx);
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
				return rsGetValue(iter.next());
			}

			@Override
			public void remove()
			{
				throw new UnsupportedOperationException();
			}
		};
	}

	private String rsGetKey(ResultSet rs)
	{
		try
		{
			return rs.getString(1);
		}
		catch (SQLException e)
		{
			throw new RuntimeException(e);
		}
	}

	private V rsGetValue(ResultSet rs)
	{
		try
		{
			return serializer.bytesToObj(rs.getBytes(2), valueClass);
		}
		catch (SQLException e)
		{
			throw new RuntimeException(e);
		}
	}

	private Iterator<KeyValue<String, V>> entryIteratorOfBytesIter(final Iterator<KeyValue<String, byte[]>> iter)
	{
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
				final KeyValue<String, byte[]> rkv = iter.next();
				return new KeyValue<String, V>()
				{
					@Override
					public String getKey()
					{
						return rkv.getKey();
					}

					@Override
					public V getValue()
					{
						return serializer.bytesToObj(rkv.getValue(), valueClass);
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

	private Integer upsertRow(final String key, final V value, final boolean strictInsert)
	{
		return jdbc.execute(new ConnListener<Integer>()
		{
			@Override
			public Integer onConnection(Connection conn) throws SQLException
			{
				PreparedStatement stmt = conn.prepareStatement(sqlUpsert(strictInsert));
				stmt.setString(1, key);
				stmt.setBytes(2, serializer.objToBytes(value));
				for (int i = 0; i < indexes.size(); i++)
				{
					SqliteIndexImpl<?> index = indexes.get(i);
					String field = (value == null) ? null : index.getIndexedFieldOf(value);
					stmt.setString(3 + i, field);
				}
				return stmt.executeUpdate();
			}
		});
	}

	/***********************************************************************************
	 * 
	 * SERIALIZATION
	 * 
	 ***********************************************************************************/

	public static interface Serializer<V>
	{
		V bytesToObj(byte[] bytes, Class<V> clz);

		byte[] objToBytes(V obj);

		V copyOf(V obj);
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

		public V copyOf(V obj)
		{
			return SerializeUtil.copyOf(obj);
		}
	}

	/***********************************************************************************
	 * 
	 * CACHE IMPLEMENTATION
	 * 
	 ***********************************************************************************/

	private class InMemCache implements Cache<String, V>
	{
		// based on Guava cache
		private com.google.common.cache.Cache<String, V> cache = com.google.common.cache.CacheBuilder.newBuilder().maximumSize(1000)
				.build();

		@Override
		public V get(String key)
		{
			return cache.getIfPresent(key);
		}

		@Override
		public Map<String, V> get(Collection<String> keys)
		{
			return cache.getAllPresent(keys);
		}

		@Override
		public void put(String key, V value)
		{
			cache.put(key, value);
		}

		@Override
		public void put(Map<String, V> values)
		{
			cache.putAll(values);
		}

		@Override
		public void delete(String key)
		{
			cache.invalidate(key);
		}

		@Override
		public void deleteAll()
		{
			cache.invalidateAll();
		}
	};
}
