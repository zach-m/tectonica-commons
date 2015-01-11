/*
 * Copyright (C) 2014 Zach Melamed
 * 
 * Latest version available online at https://github.com/zach-m/tectonica-commons
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.tectonica.util;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Miscellaneous string utility functions
 * 
 * @author Zach Melamed
 */
public class STR
{
	/**
	 * returns a comma-separated string, listing the {@code toString()} of a collection's items
	 */
	public static <V> String implode(Iterable<V> values)
	{
		return implode(values, ", ", false);
	}

	/**
	 * returns a delimiter-separated string, listing the {@code toString()} of a collection's items
	 */
	public static <V> String implode(Iterable<V> values, String delimiter, boolean skipBlanks)
	{
		if (values == null)
			return "";

		StringBuilder sb = new StringBuilder();
		boolean firstColumn = true;
		for (V value : values)
		{
			if (value == null)
				continue;
			String valueAsStr = value.toString();
			if (skipBlanks && valueAsStr.isEmpty())
				continue;
			if (firstColumn)
				firstColumn = false;
			else
				sb.append(delimiter);
			sb.append(valueAsStr);
		}
		return sb.toString();
	}

	/**
	 * returns a delimiter-separated string, listing the {@code toString()} of a collection's items
	 */
	public static <V> String implode(final V[] values, String delimiter, boolean skipBlanks)
	{
		if (values == null)
			return "";
		
		Iterable<V> iterable = new Iterable<V>()
		{
			@Override
			public Iterator<V> iterator()
			{
				return new ArrayIterator<V>(values);
			}
		};
		return implode(iterable, delimiter, skipBlanks);
	}

	private static class ArrayIterator<V> implements Iterator<V>
	{
		private final V[] array;
		private int pos = 0;

		public ArrayIterator(final V[] array)
		{
			super();
			this.array = array;
			this.pos = 0;
		}

		@Override
		public boolean hasNext()
		{
			return (pos < array.length);
		}

		@Override
		public V next()
		{
			if (!hasNext())
				throw new NoSuchElementException();
			return array[pos++];
		}

		@Override
		public void remove()
		{
			throw new UnsupportedOperationException();
		}
	}

	/**
	 * duplicates a string into a delimiter-separated format given a requested count
	 */
	public static <V> String implode(V value, String delimiter, int count)
	{
		if (count <= 0)
			return "";

		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < count - 1; i++)
			sb.append(value).append(delimiter);
		sb.append(value);
		return sb.toString();
	}

	public static String left(String s, int count)
	{
		if (s == null)
			return null;

		return s.substring(s.length() - Math.min(s.length(), count));
	}

	public static String right(String s, int count)
	{
		if (s == null)
			return null;

		return s.substring(0, Math.min(s.length(), count));
	}

	public static String leftPad(String str, int targetLength, char padChar)
	{
		return pad(str, targetLength, padChar, true);
	}

	public static String rightPad(String str, int targetLength, char padChar)
	{
		return pad(str, targetLength, padChar, false);
	}

	public static String pad(String str, int targetLength, char padChar, boolean padLeft)
	{
		if (str == null || str.isEmpty())
			return duplicate(padChar, targetLength);

		int padsCount = targetLength - str.length();

		if (padsCount <= 0)
			return str; // returns original String if longer than target length

		String padStr = duplicate(padChar, padsCount);
		return padLeft ? padStr.concat(str) : str.concat(padStr);
	}

	public static String duplicate(char padChar, int repeat)
	{
		if (repeat <= 0)
			return "";

		final char[] buf = new char[repeat];
		for (int i = 0; i < buf.length; i++)
			buf[i] = padChar;

		return new String(buf);
	}

	public static int toInt(String s, int defaultValue)
	{
		try
		{
			return Integer.parseInt(s);
		}
		catch (Exception e)
		{
			return defaultValue;
		}
	}

	public static float toFloat(String s, float defaultValue)
	{
		try
		{
			return Float.parseFloat(s);
		}
		catch (Exception e)
		{
			return defaultValue;
		}
	}

	public static double toDouble(String s, double defaultValue)
	{
		try
		{
			return Double.parseDouble(s);
		}
		catch (Exception e)
		{
			return defaultValue;
		}
	}

	public static boolean toBoolean(String s, boolean defaultValue)
	{
		boolean isFalse = (s == null) || "false".equalsIgnoreCase(s) || "0".equals(s);
		return !isFalse;
	}
}
