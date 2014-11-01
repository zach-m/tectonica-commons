package com.tectonica.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Enumeration;
import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

public class ServletUtil
{
	public static void applyCORS(HttpServletRequest req, HttpServletResponse res)
	{
		res.setHeader("Access-Control-Allow-Origin", "*");
		res.setHeader("Access-Control-Allow-Methods", "GET, POST, OPTIONS");

		String acrh = req.getHeader("Access-Control-Request-Headers");
		if (acrh != null && !acrh.isEmpty())
			res.setHeader("Access-Control-Allow-Headers", acrh);
	}

	public static String baseUrlOf(HttpServletRequest req)
	{
		String url = req.getRequestURL().toString();
		return url.substring(0, url.length() - req.getRequestURI().length()) + req.getContextPath();
	}

	public static RepostResponse repost(String url, HttpServletRequest req)
	{
		if (!req.getMethod().equals("POST"))
			throw new IllegalArgumentException("Only POST requests are supported, not " + req.getMethod());

		HttpURLConnection conn = null;
		try
		{
			long timeBefore = System.currentTimeMillis();
			conn = (HttpURLConnection) (new URL(url)).openConnection();

			// prepare request
			conn.setUseCaches(false);
			conn.setDoInput(true);
			conn.setDoOutput(true);
			conn.setRequestMethod("POST");
			Enumeration<?> headerNames = req.getHeaderNames();
			while (headerNames.hasMoreElements())
			{
				String name = headerNames.nextElement().toString();
				String value = req.getHeader(name);
				conn.setRequestProperty(name, value);
			}

			// transfer data
			int totalBytesCopied = copyStream(req.getInputStream(), conn.getOutputStream());

			// process response
			Map<String, List<String>> responseHeaders = conn.getHeaderFields();
			int statusCode = conn.getResponseCode();
			boolean statusOK = (statusCode / 100 == 2);
			InputStream is = statusOK ? conn.getInputStream() : conn.getErrorStream();
			String content = (is == null) ? "" : streamToContent(is);
			long latency = System.currentTimeMillis() - timeBefore;
			return new RepostResponse(statusCode, statusOK, content, totalBytesCopied, responseHeaders, latency);
		}
		catch (Exception e)
		{
			throw new RuntimeException(e);
		}
		finally
		{
			if (conn != null)
				conn.disconnect();
		}
	}

	private static int copyStream(InputStream in, OutputStream out) throws IOException
	{
		int totalBytesCopied = 0;
		byte[] buffer = new byte[16384];
		int bytesRead = -1;
		while ((bytesRead = in.read(buffer)) != -1)
		{
			out.write(buffer, 0, bytesRead);
			totalBytesCopied += bytesRead;
		}
		out.flush();
		in.close();
		return totalBytesCopied;
	}

	private static String streamToContent(InputStream is) throws IOException
	{
		StringBuffer sb = new StringBuffer();

		BufferedReader in = new BufferedReader(new InputStreamReader(is));
		String line;
		while ((line = in.readLine()) != null)
			sb.append(line).append("\n");
		in.close();

		return (sb.length() == 0) ? "" : sb.substring(0, sb.length() - "\n".length());
	}

	public static class RepostResponse
	{
		public final int statusCode;
		public final boolean statusOK;
		public final String content;
		public final int totalBytesCopied;
		public final Map<String, List<String>> headers;
		public final long latency;

		public RepostResponse(int statusCode, boolean statusOK, String content, int totalBytesCopied, Map<String, List<String>> headers, long latency)
		{
			this.statusCode = statusCode;
			this.statusOK = statusOK;
			this.content = content;
			this.totalBytesCopied = totalBytesCopied;
			this.headers = headers;
			this.latency = latency;
		}

		@Override
		public String toString()
		{
			return "(HTTP " + statusCode + "): [" + content + "]\n" + headers.toString();
		}
	}
}
