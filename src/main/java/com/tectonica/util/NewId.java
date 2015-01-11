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

import java.util.Random;

public class NewId
{
	private static Random rand = new Random();

	/**
	 * returns a so-called Time-UUID, i.e. a UUID that guarantees no conflicts at the following probabilities:
	 * <ul>
	 * <li>99.9999973% (or 1 - 2.7e-8) if called 1,000,000 times in every millisecond
	 * <li>value of (1 - 2.7e-14) if called once in every microsecond
	 * <li>either way, UUIDs within different milliseconds have 0% probability of conflicting
	 * </ul>
	 * The resulting UUID when used in sort retains order of creation
	 */
	public static String generate()
	{
		String timePart = Long.toHexString(System.currentTimeMillis());

		// all dates between 2004 and and 2527 are taking up 11 hex digits, so the following is unnecessary
//		timePart = leftPadded(timePart, 11, '0');

		String randomPart = boxed(Long.toHexString(rand.nextLong()), 16, '0');

		return timePart + randomPart;
	}

	private static String boxed(String str, int targetLength, char padChar)
	{
		int padsCount = targetLength - str.length();

		if (padsCount == 0)
			return str; // no alterations required

		if (padsCount < 0)
			return str.substring(-padsCount, str.length());

		final char[] pad = new char[padsCount];
		for (int i = 0; i < pad.length; i++)
			pad[i] = padChar;

		return (new String(pad)).concat(str);
	}

	/**
	 * convenience method for adding prefix to the generated id
	 */
	public static String generate(String prefix)
	{
		return prefix + generate();
	}

	/**
	 * generates a limited-length key with slim chances of being globally unique. uses all digits and characters (i.e. not hexadecimal)
	 */
	public static String generateLimited(int length)
	{
		return boxed(Long.toString(rand.nextLong(), 36), length, '0');
	}
}
