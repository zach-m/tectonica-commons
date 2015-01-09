package com.tectonica.test;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.tectonica.collections.AutoEvictMap;
import com.tectonica.collections.AutoEvictMap.Factory;

public class TestAutoEvictMap
{
	@Before
	public void setUp() throws Exception
	{}

	@After
	public void tearDown() throws Exception
	{}

	@Test
	public void test() throws Exception
	{
		final AutoEvictMap<String, String> map = new AutoEvictMap<>(new Factory<String, String>()
		{
			@Override
			public String valueOf(String key)
			{
				System.out.println(">>>> Generating for " + key);
				try
				{
					Thread.sleep(500);
				}
				catch (InterruptedException e)
				{
				}
				System.out.println("<<<< " + key);
				return "Lock for " + key;
			}
		});

		final String key = "key";
		ExecutorService exec = Executors.newFixedThreadPool(5);
		for (int i = 1; i <= 5; i++)
		{
//			final String key = "key-" + i;
			exec.execute(new Runnable()
			{
				@Override
				public void run()
				{
					try
					{
						int count = 5;
						map.acquire(key);
						for (int j = 0; j < count; j++)
						{
							map.acquire(key);
							map.acquire(key);
							map.release(key);
						}
						for (int j = 0; j < count; j++)
						{
							map.release(key);
						}
						boolean finalRelease = map.release(key);
						System.out.println("finalRelease = " + finalRelease);
					}
					catch (InterruptedException e)
					{
						e.printStackTrace();
					}
				}
			});
		}

		exec.shutdown();
		exec.awaitTermination(1, TimeUnit.HOURS);

//		fail("Not yet implemented");
	}

}
