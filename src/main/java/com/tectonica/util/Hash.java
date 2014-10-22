package com.tectonica.util;

import java.nio.charset.Charset;
import java.security.MessageDigest;

/**
 * Simple utility class for String-based hashing
 */
public class Hash
{
	public static String md5(String... pieces)
	{
		try
		{
			Charset utf = Charset.forName("UTF-8");

			MessageDigest digest = MessageDigest.getInstance("MD5");
			for (String piece : pieces)
				digest.update(piece.getBytes(utf));

			byte[] bytes = digest.digest();

			StringBuffer sb = new StringBuffer();
			for (int i = 0; i < bytes.length; ++i)
				sb.append(Integer.toHexString((bytes[i] & 0xFF) | 0x100).substring(1, 3));

			return sb.toString();
		}
		catch (Exception e)
		{
			throw new RuntimeException("Unexpected error: " + e.getMessage(), e);
		}
	}
}
