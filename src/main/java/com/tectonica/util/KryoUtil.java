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
			configure(kryo);
			output = new Output(1024, -1);
		}
	}

	public static interface Configurator
	{
		void configure(Kryo kryo);
	}

	private static Configurator defaultConfigurator = new Configurator()
	{
		@Override
		public void configure(Kryo kryo)
		{
			kryo.setReferences(false); // faster, but not suitable when cross-references are used (especially cyclic)
//			kryo.setDefaultSerializer(CompatibleFieldSerializer.class);
		}
	};

	private static void configure(Kryo kryo)
	{
		// TODO: allow an SPI for custom configuration
		defaultConfigurator.configure(kryo);
	}

	private static ThreadLocal<KryoBox> kryos = new ThreadLocal<KryoBox>()
	{
		protected KryoBox initialValue()
		{
			return new KryoBox();
		}
	};

	public static byte[] objToBytes(Object obj)
	{
		if (obj == null)
			return null;
		KryoBox kryoBox = kryos.get();
		kryoBox.output.clear();
		kryoBox.kryo.writeObject(kryoBox.output, obj);
		return kryoBox.output.toBytes();
	}

	public static <V> V bytesToObj(byte[] bytes, Class<V> clz)
	{
		if (bytes == null)
			return null;
		return kryos.get().kryo.readObject(new Input(bytes), clz);
	}

	public static <V> V copyOf(V obj)
	{
		if (obj == null)
			return null;
		return kryos.get().kryo.copy(obj);
	}

	// ////////////////////////////////////////////////////////////////////////

	private static Kryo kryo = new Kryo();
	private static Output output = new Output(1024, -1);
	static
	{
		configure(kryo);
	}

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
