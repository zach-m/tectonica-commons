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

package com.tectonica.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;

/**
 * Convenience wrapper for the JDK serialization services
 * 
 * @author Zach Melamed
 */
public class SerializeUtil
{
	public static byte[] objToBytes(Object obj)
	{
		if (obj == null)
			return null;

		try (ByteArrayOutputStream bos = new ByteArrayOutputStream())
		{
			try (ObjectOutput out = new ObjectOutputStream(bos))
			{
				out.writeObject(obj);
				return bos.toByteArray();
			}
			catch (IOException e)
			{
				throw new RuntimeException(e);
			}
		}
		catch (IOException e)
		{
			throw new RuntimeException(e);
		}
	}

	public static Object bytesToObj(byte[] bytes)
	{
		if (bytes == null)
			return null;

		try (ByteArrayInputStream bis = new ByteArrayInputStream(bytes);)
		{
			try (ObjectInput in = new ObjectInputStream(bis);)
			{
				return in.readObject();
			}
			catch (IOException | ClassNotFoundException e)
			{
				throw new RuntimeException(e);
			}
		}
		catch (IOException e)
		{
			throw new RuntimeException(e);
		}
	}

	@SuppressWarnings("unchecked")
	public static <V> V bytesToObj(byte[] bytes, Class<V> clz)
	{
		return (V) bytesToObj(bytes);
	}

	/**
	 * an inefficient way of cloning a Serializable object. use only temporarily. better solutions include frameworks like Kryo.
	 */
	@SuppressWarnings("unchecked")
	public static <V> V copyOf(V obj)
	{
		if (obj == null)
			return null;
		return (V) bytesToObj(objToBytes(obj));
	}
}
