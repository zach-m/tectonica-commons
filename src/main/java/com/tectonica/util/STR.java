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
}
