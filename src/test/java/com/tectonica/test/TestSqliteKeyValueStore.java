package com.tectonica.test;

import com.tectonica.kvs.AbstractKeyValueStore;
import com.tectonica.kvs.SqliteKeyValueStore;

public class TestSqliteKeyValueStore extends TestKeyValueStore
{
	@Override
	protected AbstractKeyValueStore<String, Topic> createStore()
	{
		return new SqliteKeyValueStore<>(Topic.class, connStr(), keyMapper);
	}

	private String connStr()
	{
		String dbPath = TestKeyValueStore.class.getResource("/").getPath() + "test.db";
		System.out.println(dbPath);
		return "jdbc:sqlite:" + dbPath;
	}
}
