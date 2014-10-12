package com.tectonica.util;

import java.io.IOException;
import java.io.PushbackReader;
import java.io.Reader;
import java.nio.CharBuffer;
import java.util.HashMap;
import java.util.Map;

public class SearchReplaceReader extends Reader
{
	protected PushbackReader pushbackReader = null;
	protected TokenResolver tokenResolver = null;
	protected StringBuilder tokenNameBuffer = new StringBuilder();
	protected String tokenValue = null;
	protected int tokenValueIndex = 0;

	public SearchReplaceReader(Reader source, TokenResolver resolver)
	{
		this.pushbackReader = new PushbackReader(source, 2);
		this.tokenResolver = resolver;
	}

	public int read(CharBuffer target) throws IOException
	{
		throw new RuntimeException("Operation Not Supported");
	}

	public int read() throws IOException
	{
		if (tokenValue != null)
		{
			if (tokenValueIndex < tokenValue.length())
				return tokenValue.charAt(tokenValueIndex++);

			if (tokenValueIndex == tokenValue.length())
			{
				tokenValue = null;
				tokenValueIndex = 0;
			}
		}

		int data = pushbackReader.read();
		if (data != '$')
			return data;

		data = pushbackReader.read();
		if (data != '{')
		{
			pushbackReader.unread(data);
			return '$';
		}
		tokenNameBuffer.delete(0, tokenNameBuffer.length());

		data = pushbackReader.read();
		while (data != '}')
		{
			tokenNameBuffer.append((char) data);
			data = pushbackReader.read();
		}

		tokenValue = tokenResolver.resolveToken(tokenNameBuffer.toString());

		if (tokenValue == null)
			tokenValue = "${" + tokenNameBuffer.toString() + "}";

		return tokenValue.charAt(tokenValueIndex++);
	}

	public int read(char cbuf[]) throws IOException
	{
		return read(cbuf, 0, cbuf.length);
	}

	public int read(char cbuf[], int off, int len) throws IOException
	{
		int charsRead = 0;
		for (int i = 0; i < len; i++)
		{
			int nextChar = read();
			if (nextChar == -1)
			{
				if (charsRead == 0)
					charsRead = -1;
				break;
			}
			charsRead = i + 1;
			cbuf[off + i] = (char) nextChar;
		}
		return charsRead;
	}

	public void close() throws IOException
	{
		pushbackReader.close();
	}

	public long skip(long n) throws IOException
	{
		throw new RuntimeException("Operation Not Supported");
	}

	public boolean ready() throws IOException
	{
		return pushbackReader.ready();
	}

	public boolean markSupported()
	{
		return false;
	}

	public void mark(int readAheadLimit) throws IOException
	{
		throw new RuntimeException("Operation Not Supported");
	}

	public void reset() throws IOException
	{
		throw new RuntimeException("Operation Not Supported");
	}

	// //////////////////////////////////////////////////////////////////////////////////

	public static interface TokenResolver
	{
		public String resolveToken(String tokenName);
	}

	public static class MapTokenResolver implements TokenResolver
	{
		protected Map<String, String> tokenMap = new HashMap<String, String>();

		public MapTokenResolver(Map<String, String> tokenMap)
		{
			this.tokenMap = tokenMap;
		}

		public String resolveToken(String tokenName)
		{
			return this.tokenMap.get(tokenName);
		}
	}
}
