package com.tectonica.util;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import javax.xml.bind.DatatypeConverter;

/**
 * General HTTP utility class for invoking HTTP requests using only Java's standard network packages (in particular, avoiding Apache Commons
 * HttpClient). The class offers a single static method {@link #execute(String, String, String, Headers, Attachment, CookieStore)}, or, for
 * better convenience, a builder pattern, accessible via {@link HTTP#url(String)}
 * 
 * @author Zach Melamed
 */
public class HTTP
{
	public static class HttpResponse
	{
		public final int statusCode;
		public final String content;
		public final Map<String, List<String>> headers;
		public final long latency;

		public HttpResponse(int statusCode, String content, Map<String, List<String>> headers, long latency)
		{
			this.statusCode = statusCode;
			this.content = content;
			this.headers = headers;
			this.latency = latency;
		}

		@Override
		public String toString()
		{
			return "(HTTP " + statusCode + "): [" + content + "]\n" + headers.toString();
		}
	}

	public static class Headers implements Iterable<Entry<String, String>>
	{
		private Map<String, String> headers = new LinkedHashMap<>();

		public void add(String headerName, String headerValue, boolean exclusive)
		{
			String value = headers.get(headerName);
			if (value == null || exclusive)
				value = headerValue;
			else
				value += ", " + headerValue;
			headers.put(headerName, value);
		}

		@Override
		public Iterator<Entry<String, String>> iterator()
		{
			return headers.entrySet().iterator();
		}
	}

	public static class Attachment
	{
		public final String fieldName;
		public final File file;

		public Attachment(String fieldName, File file)
		{
			this.fieldName = fieldName;
			this.file = file;
		}
	}

	public static class CookieStore
	{
		private Map<String, String> cookies = new HashMap<String, String>();

		private void fromHeader(List<String> setCookies)
		{
			if (setCookies != null && !setCookies.isEmpty())
			{
				for (String cookie : setCookies)
				{
					String[] kv = cookie.split(";")[0].trim().split("=");
					if (kv[1].isEmpty())
						cookies.remove(kv[0]);
					else
						cookies.put(kv[0], kv[1]);
				}
			}
		}

		private String toHeader()
		{
			if (isEmpty())
				return null;
			StringBuilder sb = new StringBuilder();
			boolean first = true;
			for (Iterator<Entry<String, String>> iter = cookies.entrySet().iterator(); iter.hasNext();)
			{
				Entry<String, String> kv = iter.next();
				if (first)
					first = false;
				else
					sb.append(",");
				sb.append(kv.getKey()).append("=").append(kv.getValue());
			}
			return sb.toString();
		}

		public boolean isEmpty()
		{
			return cookies.isEmpty();
		}
	}

	private static final String CRLF = "\r\n";
	private static final String HYPHENS = "--";
	private static final String BOUNDARY = "*************";

	public static HttpResponse execute(String method, String url, String body, Headers headers, Attachment attachment, CookieStore cs)
	{
		HttpURLConnection conn = null;
		try
		{
			boolean sendText = (body != null && !body.isEmpty());
			boolean sendMultipart = !sendText && (attachment != null && (attachment.file != null));

			long timeBefore = System.currentTimeMillis();

			conn = (HttpURLConnection) (new URL(url)).openConnection();
			conn.setUseCaches(false);
			conn.setDoInput(true);
			conn.setDoOutput(sendText || sendMultipart);

			conn.setRequestMethod(method);
			for (Entry<String, String> header : headers)
				conn.setRequestProperty(header.getKey(), header.getValue());
			if (cs != null && !cs.isEmpty())
				conn.setRequestProperty("Cookie", cs.toHeader());
//			conn.setRequestProperty("Connection", "Keep-Alive");
//			conn.setRequestProperty("Cache-Control", "no-cache");

			// send the request's body
			if (sendText)
			{
				byte[] requestContent = body.getBytes();
				conn.setRequestProperty("Content-Length", "" + requestContent.length);
				OutputStream out = conn.getOutputStream();
				out.write(requestContent);
				out.close();
			}
			else if (sendMultipart)
			{
				conn.setRequestProperty("Content-Type", "multipart/form-data;boundary=" + BOUNDARY);
				DataOutputStream out = new DataOutputStream(conn.getOutputStream());
				writeFile(attachment.file, attachment.fieldName, out); // can be done repeatedly with other files
				out.writeBytes(HYPHENS + BOUNDARY + HYPHENS + CRLF);
				out.flush();
				out.close();
			}

//			conn.connect(); // redundant, just clearer
			Map<String, List<String>> responseHeaders = conn.getHeaderFields();
			if (cs != null)
				cs.fromHeader(responseHeaders.get("Set-Cookie"));

			// get the response
			int statusCode = conn.getResponseCode();
			InputStream is = (statusCode / 100 == 2) ? conn.getInputStream() : conn.getErrorStream();

//			conn.disconnect();

			// remove the trailing NL

			String content = (is == null) ? "" : streamToContent(is);
			long latency = System.currentTimeMillis() - timeBefore;
			return new HttpResponse(statusCode, content, responseHeaders, latency);
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

	public static String urlEncoded(String url)
	{
		try
		{
			return URLEncoder.encode(url, "UTF-8").replace("+", "%20");
		}
		catch (UnsupportedEncodingException e)
		{
			return null; // never happens as "UTF-8" is always a valid encoding
		}
	}

	private static String streamToContent(InputStream is) throws IOException
	{
		BufferedReader in = new BufferedReader(new InputStreamReader(is));
		StringBuffer sb = new StringBuffer();
		String line;
		while ((line = in.readLine()) != null)
			sb.append(line).append("\n");
		String content = (sb.length() == 0) ? "" : sb.substring(0, sb.length() - "\n".length());
		in.close();
		return content;
	}

	private static void writeFile(File file, String fieldName, DataOutputStream out) throws IOException, FileNotFoundException
	{
		out.writeBytes(HYPHENS + BOUNDARY + CRLF);
		out.writeBytes("Content-Disposition: form-data; name=\"" + fieldName + "\";filename=\"" + file.getName() + "\"" + CRLF);
		out.writeBytes(CRLF);
		FileInputStream upload = new FileInputStream(file);
		byte[] buffer = new byte[4096];
		int bytesRead = -1;
		while ((bytesRead = upload.read(buffer)) != -1)
			out.write(buffer, 0, bytesRead);
		out.flush();
		upload.close();
		out.writeBytes(CRLF);
	}

	// /////////////////////////////////////////////////////////////////////////////////
	//
	// BUILDER API
	//
	// /////////////////////////////////////////////////////////////////////////////////

	private final String url;
	private String body = null;
	private Attachment attachment = null;
	private CookieStore cookieStore = null;
	private Headers headers = new Headers();

	public static HTTP url(String url)
	{
		return new HTTP(url);
	}

	public static HTTP url(String url, boolean encodeUrl)
	{
		return new HTTP(urlEncoded(url));
	}

	private HTTP(String url)
	{
		this.url = url;
	}

	public HTTP body(String body)
	{
		this.body = body;
		return this;
	}

	public HTTP contentType(String contentTypeHeader)
	{
		return header("Content-Type", contentTypeHeader, true);
	}

	public HTTP accept(String acceptHeader)
	{
		return header("Accept", acceptHeader, false);
	}

	public HTTP basicAuthorization(String username, String password)
	{
		String login = username + ":" + password;
		String encodedLogin = DatatypeConverter.printBase64Binary(login.getBytes());
		return header("Authorization", "Basic " + encodedLogin, true);
	}

	public HTTP header(String headerName, String headerValue)
	{
		return header(headerName, headerValue, false);
	}

	public HTTP header(String headerName, String headerValue, boolean exclusive)
	{
		headers.add(headerName, headerValue, exclusive);
		return this;
	}

	public HTTP cookieStore(CookieStore cookieStore)
	{
		this.cookieStore = cookieStore;
		return this;
	}

	public HTTP attach(Attachment attachment)
	{
		this.attachment = attachment;
		return this;
	}

	// //////////////////////////////////////////////////////////////////////////////////

	private HttpResponse execute(String method)
	{
		return execute(method, url, body, headers, attachment, cookieStore);
	}

	public HttpResponse GET()
	{
		return execute("GET");
	}

	public HttpResponse POST()
	{
		return execute("POST");
	}

	public HttpResponse PUT()
	{
		return execute("PUT");
	}

	public HttpResponse DELETE()
	{
		return execute("DELETE");
	}
}
