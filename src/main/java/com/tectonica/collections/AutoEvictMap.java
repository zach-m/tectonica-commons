package com.tectonica.collections;

import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;

/**
 * Concurrent, thread-safe container, retaining a key-value map with a reference count mechanism. Instead of the standard {@code get},
 * {@code put} and {@code remove} methods, if offers the following:
 * <ul>
 * <li>{@code acquire} - gets a value given a key, creating it if necessary. If the key already exists, its reference count is increased
 * <li>{@code release} - decreases a reference count of a value, removing it if the number of acquires matches the number of releases
 * </ul>
 * 
 * of course, both actions are performed atomically.
 * <p>
 * When the class is constructed a default {@link Factory} may be provided, which will generate the values when {@link #acquire(Object)}
 * needs them. Alternatively, on each invocation of {@link #acquire(Object, Factory)}, a custom ad-hoc factory may be provided for the
 * particular key acquired. When keys are released, no additional action is taken.
 * 
 * @author Zach Melamed
 */
public class AutoEvictMap<K, V>
{
	public static interface Factory<K, V>
	{
		V valueOf(K key);
	}

	private final ConcurrentMap<K, Holder<K, V>> map = new ConcurrentHashMap<K, Holder<K, V>>();
	private final Factory<K, V> defaultFactory;

	public AutoEvictMap()
	{
		this.defaultFactory = null;
	}

	public AutoEvictMap(Factory<K, V> defaultFactory)
	{
		this.defaultFactory = defaultFactory;
	}

	public V acquire(final K key) throws InterruptedException
	{
		if (defaultFactory == null)
			throw new NullPointerException("defaultFactory");

		return acquire(key, defaultFactory);
	}

	public V acquire(final K key, final Factory<K, V> customFactory) throws InterruptedException
	{
		if (key == null)
			throw new NullPointerException("key");
		if (customFactory == null)
			throw new NullPointerException("customFactory");

		Holder<K, V> holder;
		while (true)
		{
			holder = map.get(key);
			if (holder == null)
			{
				if (map.putIfAbsent(key, holder = new Holder<K, V>(key, customFactory)) == null)
				{
					// initial creation of the value
					holder.run();
					break;
				}
			}
			else
			{
				if (map.replace(key, holder, holder = holder.inc()))
					break; // ref-count increased
			}
		}

		return holder.get(); // NOTE: think whether to remove from map in case of exception/cancellation
	}

	public boolean release(K key)
	{
		if (key == null)
			throw new NullPointerException("key");

		while (true)
		{
			Holder<K, V> holder = map.get(key);
			if (holder == null)
				return true; // was already removed
			if (holder.isInitial())
			{
				if (map.remove(key, holder.initial()))
					return true; // removed now
			}
			else
			{
				if (map.replace(key, holder, holder.dec()))
					return false; // not removed, just decreased ref-count
			}
		}
	}

	public int size()
	{
		return map.size();
	}

	public void clear()
	{
		map.clear();
	}

	// /////////////////////////////////////////////////////////////////////////////////////////

	private static class Holder<K, V>
	{
		private final FutureTask<V> ft;
		private final int refCount;

		public Holder(final K key, final Factory<K, V> generator)
		{
			ft = new FutureTask<V>(new Callable<V>()
			{
				public V call() throws InterruptedException
				{
					return generator.valueOf(key);
				}
			});
			refCount = 1;
		}

		private Holder(FutureTask<V> ft, int refCount)
		{
			this.ft = ft;
			this.refCount = refCount;
		}

		public V get() throws InterruptedException
		{
			try
			{
				return ft.get();
			}
			catch (ExecutionException e)
			{
				Throwable t = e.getCause();
				if (t instanceof RuntimeException)
					throw (RuntimeException) t;
				else if (t instanceof Error)
					throw (Error) t;
				else
					throw new IllegalStateException("Not unchecked", t);
			}
		}

		public void run()
		{
			ft.run();
		}

		public Holder<K, V> inc()
		{
			return new Holder<K, V>(ft, refCount + 1);
		}

		public Holder<K, V> dec()
		{
			return new Holder<K, V>(ft, refCount - 1);
		}

		public Holder<K, V> initial()
		{
			return new Holder<K, V>(ft, 1);
		}

		public boolean isInitial()
		{
			return refCount == 1;
		}

		@Override
		@SuppressWarnings("unchecked")
		public boolean equals(Object obj)
		{
			return (refCount == ((Holder<K, V>) obj).refCount);
		}
	}
}