package com.tectonica.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.tectonica.util.KryoUtil;
import com.tectonica.util.SerializeUtil;

public class TestKryoUtil
{
	@Before
	public void setUp()
	{}

	@After
	public void tearDown()
	{}

	@Test
	public void test()
	{
		TestObj obj1 = TestObj.generate(3);
		byte[] bytes = KryoUtil.objToBytes(obj1);
		TestObj obj2 = KryoUtil.bytesToObj(bytes, TestObj.class);
		assertEquals(obj1, obj2);
		System.out.println(obj1);
		obj1.children.get(0).children.clear();
		assertNotEquals(obj1, obj2);
		TestObj obj3 = KryoUtil.copyOf(obj2);
		assertEquals(obj3, obj2);
		obj2.children.get(0).children.clear();
		assertNotEquals(obj3, obj2);
	}

	private final int REPS = 10000;
	private final int COMPLEXITY = 2;

	@Test
	@Ignore
	public void stress() throws InterruptedException
	{
		TestObj obj = TestObj.generate(COMPLEXITY);

		System.out.println("Serialize bytes: " + SerializeUtil.objToBytes(obj).length);
		System.out.println("Kryo bytes:      " + KryoUtil.objToBytes(obj).length);

		stressSerialize(obj);
		stressSerialize(obj);
		long serTime = stressSerialize(obj);

		stressSafe(obj, serTime);
		stressSafe(obj, serTime);
		stressSafe(obj, serTime);

		stressUnsafe(obj, serTime);
		stressUnsafe(obj, serTime);
		stressUnsafe(obj, serTime);
	}

	private long stressSerialize(TestObj obj) throws InterruptedException
	{
		System.gc();
		Thread.sleep(1000);
		long serBefore = System.nanoTime();
		for (int i = 0; i < REPS; i++)
			SerializeUtil.bytesToObj(SerializeUtil.objToBytes(obj), TestObj.class);
		long serTime = System.nanoTime() - serBefore;
		System.out.println("Serialize: " + (serTime / REPS));
		return serTime;
	}

	private void stressSafe(TestObj obj, long serTime) throws InterruptedException
	{
		System.gc();
		Thread.sleep(1000);
		long before = System.nanoTime();
		for (int i = 0; i < REPS; i++)
			KryoUtil.bytesToObj(KryoUtil.objToBytes(obj), TestObj.class);
		long time = System.nanoTime() - before;
		System.out.println("Kryo-Safe: " + (time / REPS) + "      Serialize / Kryo: " + (1.0 * serTime / time));
	}

	private void stressUnsafe(TestObj obj, long serTime) throws InterruptedException
	{
		System.gc();
		Thread.sleep(1000);
		long before = System.nanoTime();
		for (int i = 0; i < REPS; i++)
			KryoUtil.bytesToObj_SingleThreaded(KryoUtil.objToBytes_SingleThreaded(obj), TestObj.class);
		long time = System.nanoTime() - before;
		System.out.println("Kryo:      " + (time / REPS) + "      Serialize / Kryo: " + (1.0 * serTime / time));
	}
}

class TestObj implements Serializable
{
	private static final long serialVersionUID = 10275539472837495L;

	final boolean special;
	final double[] prices;
	final long[] quantities;
	final String msg;
	final List<TestObj> children;

	public TestObj(boolean special, double[] prices, long[] quantities, String msg, List<TestObj> children)
	{
		this.special = special;
		this.prices = prices;
		this.quantities = quantities;
		this.msg = msg;
		this.children = children;
	}

	public static Random rand = new Random();

	public static TestObj generate(int childrenCount)
	{
		boolean special = rand.nextBoolean();
		double[] prices = new double[] { rand.nextDouble(), rand.nextDouble(), rand.nextDouble() };
		long[] quantities = new long[] { rand.nextLong(), rand.nextLong() };
		String msg = Long.toHexString(rand.nextLong()) + Long.toHexString(rand.nextLong());
		List<TestObj> children = null;
		if (childrenCount > 0)
		{
			children = new ArrayList<>();
			for (int i = 0; i < childrenCount; i++)
				children.add(generate(childrenCount - 1));
		}
		return new TestObj(special, prices, quantities, msg, children);
	}

	@Override
	public int hashCode()
	{
		final int prime = 31;
		int result = 1;
		result = prime * result + ((children == null) ? 0 : children.hashCode());
		result = prime * result + ((msg == null) ? 0 : msg.hashCode());
		result = prime * result + Arrays.hashCode(prices);
		result = prime * result + Arrays.hashCode(quantities);
		result = prime * result + (special ? 1231 : 1237);
		return result;
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
		TestObj other = (TestObj) obj;
		if (children == null)
		{
			if (other.children != null)
				return false;
		}
		else if (!children.equals(other.children))
			return false;
		if (msg == null)
		{
			if (other.msg != null)
				return false;
		}
		else if (!msg.equals(other.msg))
			return false;
		if (!Arrays.equals(prices, other.prices))
			return false;
		if (!Arrays.equals(quantities, other.quantities))
			return false;
		if (special != other.special)
			return false;
		return true;
	}

	@Override
	public String toString()
	{
		return "TestObj [special=" + special + ", prices=" + Arrays.toString(prices) + ", quantities=" + Arrays.toString(quantities)
				+ ", msg=" + msg + ", children=" + children + "]";
	}
}