package com.tectonica.test;

import com.tectonica.kvs.InMemKeyValueStore;
import com.tectonica.kvs.AbstractKeyValueStore;

public class TestInMemKeyValueStore extends TestKeyValueStore
{
	@Override
	protected AbstractKeyValueStore<String, Topic> createStore()
	{
		return new InMemKeyValueStore<>(keyMapper);
	}
}
