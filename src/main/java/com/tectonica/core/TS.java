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

package com.tectonica.core;

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
