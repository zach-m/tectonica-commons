package com.tectonica.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.tectonica.kvs.Index;
import com.tectonica.kvs.Index.IndexMapper;
import com.tectonica.kvs.KeyValueStore;
import com.tectonica.kvs.KeyValueStore.KeyMapper;
import com.tectonica.kvs.Updater;

public abstract class TestKeyValueStore
{
	protected KeyValueStore<String, Topic> store;
	protected Index<String, Topic, String> bundleToTopicId;

	protected abstract KeyValueStore<String, Topic> createStore();

	protected KeyMapper<String, Topic> keyMapper = new KeyMapper<String, Topic>()
	{
		@Override
		public String getKeyOf(Topic topic)
		{
			return topic.topicId;
		}
	};

	// //////////////////////////////////////////////////////////////////////////////////////////

	@Before
	public void setUp()
	{
		store = createStore();

		bundleToTopicId = store.createIndex("b2t", new IndexMapper<Topic, String>()
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

	@Test
	public void testKVS()
	{
		Topic t1, t2, t3, t4, t;
		List<Topic> l;
		store.deleteAll();
		store.addValue(t1 = new Topic("001", "type1", TopicKind.AAA));
		store.addValue(t2 = new Topic("002", "type1", TopicKind.AAA));
		store.addValue(t3 = new Topic("003", "type3", TopicKind.AAA));
		store.addValue(t4 = new Topic("004", "type3", TopicKind.AAA));
		store.clearCache();

		assertTrue(store.containsKey("001"));
		assertFalse(store.containsKey("xxx"));

		System.err.println("-----------------------------------------------");
		System.err.println("[TEST]  " + (t = store.get("001")));
		assertEquals(t, t1);
		System.err.println("-----------------------------------------------");
		System.err.println("[TEST]  " + (l = store.valuesFor(Arrays.asList("001", "002"))));
		assertEquals(l, Arrays.asList(t1, t2));
		System.err.println("-----------------------------------------------");
		System.err.println("[TEST]  " + (l = store.valuesFor(Arrays.asList("002", "002"))));
		assertEquals(l, Arrays.asList(t2, t2));
		System.err.println("-----------------------------------------------");
		System.err.println("[TEST]  " + (l = store.valuesFor(Arrays.asList("xxx", "yyy"))));
		assertTrue(l.size() == 0);
		System.err.println("-----------------------------------------------");
		System.err.println("[TEST]  " + (l = store.valuesFor(Arrays.asList("aaa", "003", "yyy", "002", "xxx", "001", "004"))));
		assertEquals(l, Arrays.asList(t3, t2, t1, t4));
		System.err.println("-----------------------------------------------");
		System.err.println("[TEST]  " + "-- Only type3:");
		System.err.println("[TEST]  " + (l = bundleToTopicId.valuesOf(Topic.bundle("type3", TopicKind.AAA))));
		assertTrue(l.equals(Arrays.asList(t3, t4)) || l.equals(Arrays.asList(t4, t3)));
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
		System.err.println("[TEST]  " + (l = bundleToTopicId.valuesOf(Topic.bundle("type3", TopicKind.AAA))));
		assertEquals(l, Arrays.asList(t4));
		System.err.println("-----------------------------------------------");
		store.putValue(new Topic("004", "type0", TopicKind.AAA));
		System.err.println("[TEST]  " + "-- Only type3 After removal 2:");
		System.err.println("[TEST]  " + (l = bundleToTopicId.valuesOf(Topic.bundle("type3", TopicKind.AAA))));
		assertTrue(l.size() == 0);
		System.err.println("-----------------------------------------------");
		System.err.println("[TEST]  " + (t = store.get("001")));
		assertEquals(t, t1);
		System.err.println("-----------------------------------------------");
	}
}

enum TopicKind
{
	AAA, BBB;
}

class Topic implements Serializable
{
	private static final long serialVersionUID = 1L;

	public String topicId;
	public String objId;
	public TopicKind kind;

	public Topic(String topicId, String objId, TopicKind kind)
	{
		this.topicId = topicId;
		this.objId = objId;
		this.kind = kind;
	}

	public static String bundle(String objId, TopicKind kind)
	{
		return objId + "|" + kind.name();
	}

	public String bundle()
	{
		return bundle(objId, kind);
	}

	@Override
	public boolean equals(Object obj)
	{
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Topic other = (Topic) obj;
		if (kind != other.kind)
			return false;
		if (objId == null)
		{
			if (other.objId != null)
				return false;
		}
		else if (!objId.equals(other.objId))
			return false;
		if (topicId == null)
		{
			if (other.topicId != null)
				return false;
		}
		else if (!topicId.equals(other.topicId))
			return false;
		return true;
	}

	@Override
	public String toString()
	{
		return "Topic [" + topicId + ": " + objId + " [" + kind + "]";
	}
}
