package com.tectonica.test;

import java.io.Serializable;
import java.util.Arrays;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.tectonica.collections.KeyValueStore;
import com.tectonica.collections.KeyValueStore.Updater;
import com.tectonica.collections.SqliteKeyValueStore;

public class TestSqliteKeyValueStore
{
	@Before
	public void setUp()
	{
		String connStrBase = "jdbc:sqlite:" + TestSqliteKeyValueStore.class.getResource("/").getPath() + "test.db";
		store = new SqliteKeyValueStore<>(Topic.class, connStrBase, new KeyValueStore.KeyMapper<String, Topic>()
		{
			@Override
			public String getKeyOf(Topic topic)
			{
				return topic.topicId;
			}
		});

		bundleToTopicId = store.createIndex("b2t", new KeyValueStore.IndexMapper<Topic, String>()
		{
			@Override
			public String getIndexedFieldOf(Topic topic)
			{
				return topic.bundle();
			}
		});
	}

	@After
	public void tearDown()
	{}

	// //////////////////////////////////////////////////////////////////////////////////////////

	public static enum TopicKind
	{
		AAA, BBB;
	}

	public static class Topic implements Serializable
	{
		private static final long serialVersionUID = 1L;

		public String topicId;
		public String objId;
		public TopicKind kind;

		private Topic(String topicId, String objId, TopicKind kind)
		{
			this.topicId = topicId;
			this.objId = objId;
			this.kind = kind;
		}

		private static String bundle(String objId, TopicKind kind)
		{
			return objId + "|" + kind.name();
		}

		private String bundle()
		{
			return bundle(objId, kind);
		}

		@Override
		public String toString()
		{
			return "Topic [" + topicId + ": " + objId + " [" + kind + "]";
		}
	}

	private KeyValueStore<String, Topic> store;
	private KeyValueStore.Index<String, Topic, String> bundleToTopicId;

	@Test
	public void testKVS()
	{
		store.deleteAll();
		store.insertValue(new Topic("001", "type1", TopicKind.AAA));
		store.insertValue(new Topic("002", "type1", TopicKind.AAA));
		store.insertValue(new Topic("003", "type3", TopicKind.AAA));
		store.insertValue(new Topic("004", "type3", TopicKind.AAA));
		store.clearCache();
		System.err.println("-----------------------------------------------");
		System.err.println("[TEST]  " + store.get("001"));
		System.err.println("-----------------------------------------------");
		System.err.println("[TEST]  " + store.valuesFor(Arrays.asList("001", "002")));
		System.err.println("-----------------------------------------------");
		System.err.println("[TEST]  " + store.valuesFor(Arrays.asList("002", "002")));
		System.err.println("-----------------------------------------------");
		System.err.println("[TEST]  " + store.valuesFor(Arrays.asList("xxx", "yyy")));
		System.err.println("-----------------------------------------------");
		System.err.println("[TEST]  " + store.valuesFor(Arrays.asList("aaa", "003", "yyy", "002", "xxx", "001", "004")));
		System.err.println("-----------------------------------------------");
		System.err.println("[TEST]  " + "-- Only type3:");
		System.err.println("[TEST]  " + bundleToTopicId.valuesOf(Topic.bundle("type3", TopicKind.AAA)));
		System.err.println("-----------------------------------------------");
		store.update("003", new Updater<Topic>()
		{
			@Override
			public boolean update(Topic value)
			{
				value.objId = "type0";
				return true;
			}
		});
		System.err.println("[TEST]  " + "-- Only type3 After removal 1:");
		System.err.println("[TEST]  " + bundleToTopicId.valuesOf(Topic.bundle("type3", TopicKind.AAA)));
		System.err.println("-----------------------------------------------");
		store.replace("004", new Topic("004", "type0", TopicKind.AAA));
		System.err.println("[TEST]  " + "-- Only type3 After removal 2:");
		System.err.println("[TEST]  " + bundleToTopicId.valuesOf(Topic.bundle("type3", TopicKind.AAA)));
		System.err.println("-----------------------------------------------");
		System.err.println("[TEST]  " + store.get("001"));
		System.err.println("-----------------------------------------------");
		store.clearCache();
		System.err.println("[TEST]  " + store.get("001"));
		System.err.println("-----------------------------------------------");
	}
}
