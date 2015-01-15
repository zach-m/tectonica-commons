package com.tectonica.test;

import com.tectonica.kvs.KeyValueStore;
import com.tectonica.kvs.impl.InMemKeyValueStore;

public class TestInMemKeyValueStore extends TestKeyValueStore
{
	@Override
	protected KeyValueStore<String, Topic> createStore()
	{
		return new InMemKeyValueStore<>(keyMapper);
	}
}
