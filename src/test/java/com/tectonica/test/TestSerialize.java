package com.tectonica.test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import com.tectonica.util.SerializeUtil;

public class TestSerialize
{
	@Test
	public void test()
	{
		try
		{
			List<String> list1 = new ArrayList<>(Arrays.asList("Hello", "John"));
			List<String> list2 = SerializeUtil.copyOf(list1);
			list2.add("Again");
			System.out.println(list1);
			System.out.println(list2);
			Assert.assertTrue(list2.size() == list1.size() + 1);
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
	}
}
