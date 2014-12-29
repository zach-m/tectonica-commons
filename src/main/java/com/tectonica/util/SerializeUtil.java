package com.tectonica.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;

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
	 * an inefficient way of cloning a Serializable object. use only temporarily. better solutions include frameworks such as Kryo.
	 */
	@SuppressWarnings("unchecked")
	public static <V> V copyOf(V obj)
	{
		if (obj == null)
			return null;
		return (V) bytesToObj(objToBytes(obj));
	}
}
