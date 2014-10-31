package com.tectonica.util;

import java.util.Collection;

/**
 * Miscellaneous string utility functions
 * 
 * @author Zach Melamed
 */
public class STR
{
	/**
	 * returns a delimiter-separated string listing the {@code toString()} of an object collection
	 */
	public static String implode(Collection<? extends Object> values)
	{
		return implode(values, ", ");
	}

	/**
	 * returns a comma-separated string listing the {@code toString()} of an object collection
	 */
	public static String implode(Collection<? extends Object> values, String delimiter)
	{
		if (values == null)
			return "";

		StringBuilder sb = new StringBuilder();
		boolean firstColumn = true;
		for (Object value : values)
		{
			if (firstColumn)
				firstColumn = false;
			else
				sb.append(delimiter);
			sb.append(value.toString());
		}
		return sb.toString();
	}

	/**
	 * duplicates a string into a delimiter-separated format given a requested count
	 */
	public static String implode(String s, int count)
	{
		if (count <= 0)
			return "";

		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < count - 1; i++)
			sb.append(s).append(", ");
		sb.append(s);
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
}
