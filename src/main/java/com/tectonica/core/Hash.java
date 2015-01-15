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

import java.nio.charset.Charset;
import java.security.MessageDigest;
import java.util.Arrays;

/**
 * Simple utility class for String-based hashing
 */
public class Hash
{
	public static String md5(String... pieces)
	{
		return digested("MD5", pieces);
	}

	public static String sha256(String... pieces)
	{
		return digested("SHA-256", pieces);
	}

	private static String digested(String algorithm, String... pieces)
	{
		try
		{
			MessageDigest digest = MessageDigest.getInstance(algorithm);
			
			Charset utf = Charset.forName("UTF-8");
			for (String piece : pieces)
				digest.update(piece.getBytes(utf));
			
			return bytesToHex(digest.digest());
		}
		catch (Exception e)
		{
			throw new RuntimeException("Error digesting: " + Arrays.toString(pieces), e);
		}
	}

	public static String bytesToHex(byte[] bytes)
	{
		StringBuffer sb = new StringBuffer();

		for (int i = 0; i < bytes.length; ++i)
			sb.append(Integer.toHexString((bytes[i] & 0xFF) | 0x100).substring(1, 3));

		return sb.toString();
	}
}
