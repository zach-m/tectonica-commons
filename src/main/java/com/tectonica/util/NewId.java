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
//		timePart = StrUtils.leftPad(timePart, 11, '0');

		String randomPart = Long.toHexString(rand.nextLong());

		return timePart + randomPart;
	}
}
