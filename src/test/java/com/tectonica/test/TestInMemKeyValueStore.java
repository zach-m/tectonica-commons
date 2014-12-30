package com.tectonica.test;

import com.tectonica.collections.InMemKeyValueStore;
import com.tectonica.collections.KeyValueStore;

public class TestInMemKeyValueStore extends TestKeyValueStore
{
	@Override
	protected KeyValueStore<String, Topic> createStore()
	{
		return new InMemKeyValueStore<>(keyMapper);
	}
}
