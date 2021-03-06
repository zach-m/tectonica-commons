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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.Reader;

public class IO
{
	public static String streamToString(final InputStream inputStream)
	{
		final StringBuilder sb = new StringBuilder();
		streamToStringBuilder(inputStream, sb);
		return sb.toString();
	}

	public static void streamToStringBuilder(final InputStream inputStream, StringBuilder sb)
	{
		try (final Reader in = new BufferedReader(new InputStreamReader(inputStream, "UTF-8")))
		{
			final char[] buffer = new char[1024];
			int read;
			while ((read = in.read(buffer, 0, buffer.length)) != -1)
				sb.append(buffer, 0, read);
		}
		catch (Exception e)
		{
			throw new RuntimeException(e);
		}
	}

	public static void streamToFile(InputStream inputStream, String outputFileName)
	{
		try (final OutputStream out = new FileOutputStream(new File(outputFileName)))
		{
			byte[] bytes = new byte[4096];
			int read;
			while ((read = inputStream.read(bytes)) != -1)
				out.write(bytes, 0, read);
		}
		catch (Exception e)
		{
			throw new RuntimeException(e);
		}
	}
}
