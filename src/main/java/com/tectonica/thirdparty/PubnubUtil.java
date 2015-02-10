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

package com.tectonica.thirdparty;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Provides REST-based implementation of PubNub's PUBLISH API, as an alternative for using PubNub's Java SDK, which relies on an internal
 * HTTP-requests queue, that may not play well with containers such as JavaEE and GAE. The REST API itself is invoked synchronously by
 * {@link #sendText(String, String)} and {@link #sendJson(String, String)}, so avoid using them in the main thread due to network latency.
 * <p>
 * While not a requirement, this class is intended to be used as a Singleton, and it is thread-safe by design (note, however, that it allows
 * you to hook a listener, which may break the thread-safeness).
 * 
 * @see http://www.pubnub.com/http-rest-push-api/
 */
public class PubnubUtil
{
	private static final String PUBNUB_PUBLISH_URL_PREFIX = "http://pubsub.pubnub.com/publish/";

	private static final Logger LOG = LoggerFactory.getLogger(PubnubUtil.class);

	public static interface Listener
	{
		void onSuccess(String channel, String msg);

		void onError(String channel, String msg);
	}

	private Listener listener = new Listener()
	{
		@Override
		public void onSuccess(String channel, String msg)
		{
			LOG.info(msg);
		}

		@Override
		public void onError(String channel, String msg)
		{
			LOG.error(msg);
		}
	};

	public Listener getListener()
	{
		return listener;
	}

	public void setListener(Listener listener)
	{
		this.listener = listener;
	}

	// //////////////////////////////////////////////////////////////////////////////

	private final String urlFmt;

	public PubnubUtil(String publishKey, String subscribeKey)
	{
		urlFmt = PUBNUB_PUBLISH_URL_PREFIX + publishKey + "/" + subscribeKey + "/0/%s/0/%s";
	}

	public boolean sendText(String channel, String txtMsg)
	{
		return sendJson(channel, stringAsJson(txtMsg));
	}

	public boolean sendJson(String channel, String jsonMsg)
	{
		String url = String.format(urlFmt, urlEncoded(channel), urlEncoded(jsonMsg));
		HttpURLConnection conn = null;
		try
		{
			conn = (HttpURLConnection) (new URL(url)).openConnection();
			conn.setRequestMethod("GET");
			conn.setUseCaches(false);
			conn.setDoInput(true);
			conn.setDoOutput(false);
//			Map<String, List<String>> responseHeaders = conn.getHeaderFields();
			int statusCode = conn.getResponseCode();
			boolean ok = statusCode / 100 == 2;
			InputStream is = ok ? conn.getInputStream() : conn.getErrorStream();
			String content = (is == null) ? "" : streamToString(is);
			if (ok)
			{
				if (listener != null)
					listener.onSuccess(channel, content);
				return true;
			}
			else
			{
				if (listener != null)
					listener.onError(channel, content);
				return false;
			}
		}
		catch (Exception e)
		{
			e.printStackTrace();
			if (listener != null)
				listener.onError(channel, e.toString());
			return false;
		}
	}

	private final Pattern p1 = Pattern.compile("\\\\");
	private final Pattern p2 = Pattern.compile("\"");

	/**
	 * Converts a Java string into a valid JSON string literal (by escaping quotation marks and backslashes,
	 * and then surrounding with quotes)
	 * <p>
	 * NOTE: to be completely accurate, escaping is also required for tabs, newlines, backspaces, etc., but these aren't normally expected
	 * in a published text
	 * 
	 * @param text
	 *            the raw string for conversion
	 * @return
	 *         JSON-compliant string literal
	 */
	protected String stringAsJson(String text)
	{
		text = p1.matcher(text).replaceAll("\\\\\\\\"); // first escape backslashes
		text = p2.matcher(text).replaceAll("\\\\\\\""); // escape quotation marks
		return "\"" + text + "\"";
	}

	private String streamToString(InputStream is) throws IOException
	{
		StringBuffer sb = new StringBuffer();

		BufferedReader in = new BufferedReader(new InputStreamReader(is));
		String line;
		while ((line = in.readLine()) != null)
			sb.append(line).append("\n");
		in.close();

		return (sb.length() == 0) ? "" : sb.substring(0, sb.length() - "\n".length());
	}

	private String urlEncoded(String url)
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
}
