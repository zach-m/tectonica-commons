package com.tectonica.test;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.appengine.tools.development.testing.LocalDatastoreServiceTestConfig;
import com.google.appengine.tools.development.testing.LocalMemcacheServiceTestConfig;
import com.google.appengine.tools.development.testing.LocalServiceTestHelper;
import com.tectonica.gae.GaeSequence;

public class TestGaeSequence
{
	private final LocalServiceTestHelper helper = new LocalServiceTestHelper(
			new LocalDatastoreServiceTestConfig().setDefaultHighRepJobPolicyUnappliedJobPercentage(100),
			new LocalMemcacheServiceTestConfig());

	@Before
	public void setUp()
	{
		helper.setUp();
	}

	@After
	public void tearDown()
	{
		helper.tearDown();
	}

	@Test
	public void test()
	{
		final GaeSequence seq1 = new GaeSequence("xxx", "ns");
		final GaeSequence seq2 = new GaeSequence("yyy", "ns");
		final GaeSequence seq3 = new GaeSequence("xxx");
		for (int j = 1; j < 10; j++)
		{
			System.out.println(seq1.incrementAndGet());
			System.out.println(seq2.incrementAndGet());
			System.out.println(seq3.incrementAndGet());
		}
	}
}
