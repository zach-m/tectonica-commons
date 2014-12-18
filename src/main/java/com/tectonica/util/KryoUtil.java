package com.tectonica.util;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

public class KryoUtil
{
	private static class KryoBox
	{
		final Kryo kryo;
		final Output output;

		public KryoBox()
		{
			kryo = new Kryo();
//			kryo.setDefaultSerializer(CompatibleFieldSerializer.class);
			output = new Output(1024, -1);
		}
	}

	private static ThreadLocal<KryoBox> kryos = new ThreadLocal<KryoBox>()
	{
		protected KryoBox initialValue()
		{
			return new KryoBox();
		};
	};

	public static byte[] objToBytes(Object obj)
	{
		KryoBox kryoBox = kryos.get();
		kryoBox.output.clear();
		kryoBox.kryo.writeObject(kryoBox.output, obj);
		return kryoBox.output.toBytes();
	}

	public static <V> V bytesToObj(byte[] bytes, Class<V> clz)
	{
		return kryos.get().kryo.readObject(new Input(bytes), clz);
	}

	public static <V> V copyOf(V obj)
	{
		return kryos.get().kryo.copy(obj);
	}

	// ////////////////////////////////////////////////////////////////////////

	private static Kryo kryo = new Kryo();
	private static Output output = new Output(1024, -1);

	public static byte[] objToBytes_SingleThreaded(Object obj)
	{
		output.clear();
		kryo.writeObject(output, obj);
		return output.toBytes();
	}

	public static <V> V bytesToObj_SingleThreaded(byte[] bytes, Class<V> clz)
	{
		return kryo.readObject(new Input(bytes), clz);
	}

	public static <V> V copyOf_SingleThreaded(V obj)
	{
		return kryo.copy(obj);
	}
}
