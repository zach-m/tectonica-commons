package com.tectonica.util;

import java.util.Arrays;

/**
 * A memory-efficient data structure for storing a 3-dimensional bit array. The data is not compacted and the array is not assumed to be
 * sparse. However, the implementation is based on a vector of long-primitives, stored consecutively in memory and taking the least amount
 * possible of overhead footprint. The class assumes that the interesting data is stored in axis-Z, and therefore provides special APIs for
 * this particular axis.
 * 
 * @author Zach Melamed
 *
 */
public class BitCube
{
	private final static int ADDRESS_BITS_PER_WORD = 6;
	private final static int BITS_PER_WORD = 1 << ADDRESS_BITS_PER_WORD;

	private final long[] words;

	private final int xAxisSize;
	private final int yAxisSize;
	private final int zAxisSize;
	private final int zWordCount;
	private final int yWordCount;

	public BitCube(int xAxisSize, int yAxisSize, int zAxisSize)
	{
		this.xAxisSize = xAxisSize;
		this.yAxisSize = yAxisSize;
		this.zAxisSize = zAxisSize;
		zWordCount = wordLocalIndex(zAxisSize - 1) + 1;
		yWordCount = yAxisSize * zWordCount;
		words = new long[xAxisSize * yWordCount];
	}

	private int wordLocalIndex(int bitIndex)
	{
		return bitIndex >> ADDRESS_BITS_PER_WORD;
	}

	private int wordIndex(int x, int y, int z)
	{
		return (x * yWordCount) + (y * zWordCount) + wordLocalIndex(z);
	}

	public long getBufferSize() // in bytes
	{
		return (1L * words.length) << (ADDRESS_BITS_PER_WORD - 3);
	}

	public int getXAxisSize()
	{
		return xAxisSize;
	}

	public int getYAxisSize()
	{
		return yAxisSize;
	}

	public int getZAxisSize()
	{
		return zAxisSize;
	}

	private void checkDimensions(int x, int y, int z)
	{
		assert (x >= 0 && x < xAxisSize);
		assert (y >= 0 && y < yAxisSize);
		assert (z >= 0 && z < zAxisSize);
	}

	public boolean get(int x, int y, int z)
	{
		checkDimensions(x, y, z);
		int i = wordIndex(x, y, z);
		return (words[i] & (1L << z)) != 0L;
	}

	public void set(int x, int y, int z, boolean value)
	{
		checkDimensions(x, y, z);
		long mask = 1L << z;
		int i = wordIndex(x, y, z);
		if (value)
			words[i] |= mask;
		else
			words[i] &= ~mask;
	}

	public boolean getAndSet(int x, int y, int z, boolean value)
	{
		checkDimensions(x, y, z);
		long mask = 1L << z;
		int i = wordIndex(x, y, z);
		boolean wasSet = (words[i] & mask) != 0L;
		if (value)
			words[i] |= mask;
		else
			words[i] &= ~mask;
		return wasSet;
	}

	public void setAxisX(int y, int z, boolean value)
	{
		checkDimensions(0, y, z);
		long mask = 1L << z;
		for (int x = 0, i = wordIndex(x, y, z); x < xAxisSize; x++, i += yWordCount)
		{
			if (value)
				words[i] |= mask;
			else
				words[i] &= ~mask;
		}
	}

	public void setAxisY(int x, int z, boolean value)
	{
		checkDimensions(x, 0, z);
		long mask = 1L << z;
		for (int y = 0, i = wordIndex(x, y, z); y < yAxisSize; y++, i += zWordCount)
		{
			if (value)
				words[i] |= mask;
			else
				words[i] &= ~mask;
		}
	}

	public void setAxisZ(int x, int y, boolean value)
	{
		checkDimensions(x, y, 0);
		int firstWordIndex = wordIndex(x, y, 0);
		int lastWordIndex = firstWordIndex + zWordCount - 1;
		if (zWordCount > 1)
			Arrays.fill(words, firstWordIndex, lastWordIndex, value ? ~0L : 0L);
		words[lastWordIndex] = value ? (~0L >>> -zAxisSize) : 0L;
	}

	public int nextSetBitZ(int x, int y, int z)
	{
		checkDimensions(x, y, z);
		return nextSetBit(z, wordIndex(x, y, 0));
	}

	private int nextSetBit(int bitIndex, int baseWordIndex)
	{
		int u = wordLocalIndex(bitIndex);
		if (u == zWordCount)
			return -1;

		long word = words[baseWordIndex + u] & (~0L << bitIndex);

		while (true)
		{
			if (word != 0)
				return (u * BITS_PER_WORD) + Long.numberOfTrailingZeros(word);
			if (++u == zWordCount)
				return -1;
			word = words[baseWordIndex + u];
		}
	}

	/**
	 * Given X and Y, returns how bits in axis-Z are set
	 */
	public int getBitCountZ(int x, int y)
	{
		checkDimensions(x, y, 0);
		int index = wordIndex(x, y, 0);
		int bitCount = 0;
		for (int i = index; i < index + zWordCount; i++)
			bitCount += Long.bitCount(words[i]);
		return bitCount;
	}

	public static interface SearchListener
	{
		void initialize(int size);

		void add(int index);
	}

	public void searchAxisZ(int x, int y, SearchListener listener)
	{
		listener.initialize(getBitCountZ(x, y));
		int base = wordIndex(x, y, 0);
		for (int i = nextSetBit(0, base); i >= 0; i = nextSetBit(i + 1, base))
			listener.add(i);
	}

	/**
	 * Given X and Y, returns an array of indices in axis-Z where the bits are set
	 */
	public int[] getAxisZ(int x, int y)
	{
		BasicSearchListener listener = new BasicSearchListener();
		searchAxisZ(x, y, listener);
		return listener.result;
	}

	private static class BasicSearchListener implements SearchListener
	{
		int[] result;
		int counter;

		@Override
		public void initialize(int size)
		{
			result = new int[size];
			counter = 0;
		}

		@Override
		public void add(int index)
		{
			result[counter++] = index;
		}
	};
}
