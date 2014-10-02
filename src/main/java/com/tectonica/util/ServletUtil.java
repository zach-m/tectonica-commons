package com.tectonica.util;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

public class ServletUtil
{
	public static void applyCORS(HttpServletRequest req, HttpServletResponse resp)
	{
		resp.setHeader("Access-Control-Allow-Origin", "*");
		resp.setHeader("Access-Control-Allow-Methods", "GET, POST, OPTIONS");

		String acrh = req.getHeader("Access-Control-Request-Headers");
		if (acrh != null && !acrh.isEmpty())
			resp.setHeader("Access-Control-Allow-Headers", acrh);
	}

	public static String streamToString(final InputStream is)
	{
		final char[] buffer = new char[1024];
		final StringBuilder sb = new StringBuilder();
		try (final Reader in = new InputStreamReader(is, "UTF-8"))
		{
			int read;
			while ((read = in.read(buffer, 0, buffer.length)) > 0)
				sb.append(buffer, 0, read);
		}
		catch (Exception e)
		{
			e.printStackTrace();
			return null;
		}
		return sb.toString();
	}
}
