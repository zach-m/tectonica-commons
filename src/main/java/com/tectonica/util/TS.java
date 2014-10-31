package com.tectonica.util;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class TS
{
	public static Long nowL()
	{
		return System.currentTimeMillis();
	}

	public static Calendar now()
	{
		return Calendar.getInstance();
	}

	public static Calendar toCalendar(Long time)
	{
		if (time == null)
			return null;
		Calendar cal = Calendar.getInstance();
		cal.setTimeInMillis(time);
		return cal;
	}

	public static Long toLong(Calendar cal)
	{
		return (cal == null) ? null : cal.getTimeInMillis();
	}

	private static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-dd-MM HH:mm:ss");

	public static String toString(Long time)
	{
		return (time == null) ? null : sdf.format(new Date(time));
	}
}
