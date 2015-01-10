package com.tectonica.test;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.junit.Assert;
import org.junit.Test;

import com.tectonica.collections.AutoEvictMap;

public class TestAutoEvictMap
{
	@Test
	public void test() throws Exception
	{
		final AutoEvictMap<String, String> map = new AutoEvictMap<>(new AutoEvictMap.Factory<String, String>()
		{
			@Override
			public String valueOf(String key)
			{
				System.out.println(">>>> Generating value for [" + key + "]");
				try
				{
					Thread.sleep(500);
				}
				catch (InterruptedException e)
				{
				}
				System.out.println("<<<< [" + key + "]");
				return "VALUE-FOR-" + key;
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

		Assert.assertEquals(0, map.size());
	}
}
