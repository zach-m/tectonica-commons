package com.tectonica.test;

import static com.google.appengine.api.datastore.FetchOptions.Builder.withLimit;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.google.appengine.api.datastore.DatastoreService;
import com.google.appengine.api.datastore.DatastoreServiceFactory;
import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.Query;
import com.google.appengine.api.memcache.MemcacheService;
import com.google.appengine.api.memcache.MemcacheServiceFactory;
import com.google.appengine.tools.development.testing.LocalDatastoreServiceTestConfig;
import com.google.appengine.tools.development.testing.LocalMemcacheServiceTestConfig;
import com.google.appengine.tools.development.testing.LocalServiceTestHelper;
import com.tectonica.kvs.KeyValueStore;
import com.tectonica.kvs.impl.GaeKeyValueStore;
import com.tectonica.kvs.impl.GaeKeyValueStore.Config;

public class TestGaeKeyValueStore extends TestKeyValueStore
{
	private final LocalServiceTestHelper helper = new LocalServiceTestHelper(
			new LocalDatastoreServiceTestConfig().setDefaultHighRepJobPolicyUnappliedJobPercentage(100),
			new LocalMemcacheServiceTestConfig());

	@Before
	@Override
	public void setUp()
	{
		helper.setUp();
		super.setUp();
	}

	@After
	@Override
	public void tearDown()
	{
		helper.tearDown();
		super.tearDown();
	}

	@Override
	protected KeyValueStore<String, Topic> createStore()
	{
		return new GaeKeyValueStore<>(Config.create(Topic.class).withNamespace("ns"), keyMapper);
	}

	@Test
	@Ignore
	public void test()
	{
		DatastoreService ds = DatastoreServiceFactory.getDatastoreService();
		assertEquals(0, ds.prepare(new Query("yam")).countEntities(withLimit(10)));
		ds.put(new Entity("yam"));
		ds.put(new Entity("yam"));
		assertEquals(2, ds.prepare(new Query("yam")).countEntities(withLimit(10)));

		MemcacheService mc = MemcacheServiceFactory.getMemcacheService();
		assertFalse(mc.contains("yar"));
		mc.put("yar", "foo");
		assertTrue(mc.contains("yar"));
	}
}
