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

package com.tectonica.gae;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.appengine.api.memcache.Expiration;
import com.google.appengine.api.memcache.MemcacheService;
import com.google.appengine.api.memcache.MemcacheService.SetPolicy;
import com.google.appengine.api.memcache.MemcacheServiceFactory;
import com.tectonica.collections.AutoEvictMap;

/**
 * a {@link Lock} implementation for Google App Engine applications, which uses the Memcache service to ensure global locking (i.e. among
 * all the instances of the application). The lock is re-entrant (i.e. can be locked and then re-locked in a nested method by the same
 * thread) and minimizes the amount of calls to the Memcache service by having all threads from a single instance share the Memcache
 * polling loop.
 * <p>
 * Each lock object is associated with a globally-unique name, so that when separate threads (on possibly different instances) try to
 * acquire a lock with the same name, only one at the time succeeds.
 * <p>
 * The usage is straightforward
 * 
 * <pre>
 * Lock lock = GaeMemcacheLock.getLock("LOCK_GLOBAL_NAME", true, "NAMESPACE_GLOBAL_NAME");
 * lock.lock();
 * try {
 *    ...
 * }
 * finally {
 *    lock.unlock();
 * }
 * </pre>
 * 
 * Each lock attained with {@link #getLock(String, boolean, String)} must be eventually released with {@link #disposeLock(String)}. It's
 * possible however to attain a lock that's set for automatic disposal (upon its {@code unlock()}).
 * <p>
 * NOTE: Locking with Memcache is not a bullet-proof solution, as unexpected eviction of the cache may result in a situation where one
 * instance acquires a lock that is in fact taken by another. However, the risk is very minimal especially if using the Dedicated Plan from
 * Google (see <a href='https://cloud.google.com/appengine/docs/adminconsole/memcache'>here</a>). Also, a lock entry in Memcache is very
 * unlikely to be evicted due to LRU considerations, as locks are either short-lived or very frequently updated (in high contention).
 * 
 * @author Zach Melamed
 */
public class GaeMemcacheLock implements Lock
{
	private static final int LOCK_AUTO_EXPIRATION_MS = 30000;
	private static final long SLEEP_BETWEEN_RETRIES_MS = 50L;

	private static final Logger LOG = LoggerFactory.getLogger(GaeMemcacheLock.class);

	private final MemcacheService mc;
	private final String globalName;
	private final boolean disposeWhenUnlocked;
	private final long sleepBetweenRetriesMS;
	private final ReentrantLock localLock;
	private ThreadLocal<Integer> reentranceDepth = new ThreadLocal<Integer>()
	{
		@Override
		protected Integer initialValue()
		{
			return 0;
		}
	};

	private GaeMemcacheLock(String globalName, boolean disposeWhenUnlocked, String namespace, boolean locallyFair,
			long sleepBetweenRetriesMS)
	{
		this.mc = MemcacheServiceFactory.getMemcacheService(namespace);
		this.globalName = globalName;
		this.disposeWhenUnlocked = disposeWhenUnlocked;
		this.sleepBetweenRetriesMS = sleepBetweenRetriesMS;
		this.localLock = new ReentrantLock(locallyFair);
	}

	@Override
	public void lockInterruptibly() throws InterruptedException
	{
		localLock.lock();
		lockGlobally(null);
	}

	@Override
	public void lock()
	{
		try
		{
			lockInterruptibly();
		}
		catch (InterruptedException e)
		{
			throw new RuntimeException(e);
		}
	}

	@Override
	public void unlock()
	{
		unlockGlobally();
		localLock.unlock();
		if (disposeWhenUnlocked)
			disposeLock(globalName);
	}

	@Override
	public boolean tryLock()
	{
		if (!localLock.tryLock())
			return false;
		if (!tryLockGlobally())
		{
			localLock.unlock(); // since we weren't able to lock globally, no point in holding the local lock we just acquired
			return false;
		}
		return true;
	}

	@Override
	public boolean tryLock(long time, TimeUnit unit) throws InterruptedException
	{
		long timeout = System.currentTimeMillis() + unit.toMillis(time);
		if (localLock.tryLock(time, unit))
			return lockGlobally(timeout);
		return false;
	}

	@Override
	public Condition newCondition()
	{
		throw new UnsupportedOperationException();
	}

	/**
	 * Blocks until the global lock is acquired, a timeout is reached, or an interruption occurs. It does so using an infinite loop which
	 * checks a shared Memcache entry once in every short-while
	 */
	private boolean lockGlobally(Long timeout) throws InterruptedException
	{
		while (true)
		{
			if (Thread.interrupted())
				throw new InterruptedException();

			long before = System.currentTimeMillis();

			if (tryLockGlobally())
				return true;

			// check if we timed out
			long after = System.currentTimeMillis();
			if (timeout != null && after >= timeout.longValue())
				return false;

			// calculate the exact amount of sleep needed before the next retry
			long roudtripMS = after - before;
			long actualSleep = Math.max(0L, sleepBetweenRetriesMS - roudtripMS);
			if (actualSleep > 0L)
			{
				if (timeout != null)
				{
					// if we sleep as calculated, how much will we overflow beyond the timeout?
					long overflow = Math.max(0L, after + actualSleep - timeout.longValue());
					if ((actualSleep -= overflow) <= 0L)
						continue;
				}
				TimeUnit.MILLISECONDS.sleep(actualSleep);
			}
		}
	}

	/**
	 * Performs a single attempt to acquire the global lock. Supports reentrancy.
	 * 
	 * @return
	 *         true if the global lock was acquired, false otherwise (i.e. lock is acquired by another instance)
	 */
	private boolean tryLockGlobally()
	{
		int depth = reentranceDepth.get().intValue();

		boolean acquired;
		if (depth > 0)
			acquired = true;
		else
			acquired = mc.put(globalName, "X", Expiration.byDeltaMillis(LOCK_AUTO_EXPIRATION_MS), SetPolicy.ADD_ONLY_IF_NOT_PRESENT);

		if (acquired)
			reentranceDepth.set(depth + 1);

		return acquired;
	}

	/**
	 * Guaranteed to run before releasing the local lock, this method releases the global lock first. It uses a reference counting
	 * methodology to support reentrancy
	 */
	private void unlockGlobally()
	{
		int depth = reentranceDepth.get().intValue() - 1;
		reentranceDepth.set(depth);
		if (depth == 0)
		{
			final boolean deleted = mc.delete(globalName);
			if (!deleted)
				LOG.warn("Unexpected behavior of lock. consider using namespaces");
		}
	}

	@Override
	public int hashCode()
	{
		return globalName.hashCode();
	}

	@Override
	public boolean equals(Object obj)
	{
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		GaeMemcacheLock other = (GaeMemcacheLock) obj;
		return globalName.equals(other.globalName);
	}

	// //////////////////////////////////////////////////////////////////////////////////////////

	private static AutoEvictMap<String, GaeMemcacheLock> locks = new AutoEvictMap<>();

	/**
	 * calls {@link #getLock(String, boolean, String, boolean, long)} with unfair locking (more efficient) and 50ms sleep-between-retries
	 */
	public static GaeMemcacheLock getLock(String globalName, boolean disposeWhenUnlocked, String namespace)
	{
		return getLock(globalName, disposeWhenUnlocked, namespace, false, SLEEP_BETWEEN_RETRIES_MS);
	}

	/**
	 * Returns a {@link Lock} object, for global locking across all App-Engine instances (or more accurately, all that share a given
	 * namespace). When the lock is no longer needed, invoke {@link #disposeLock(String)}. Alternatively, you may pass
	 * {@code disposeWhenUnlocked = true} here to have the dispose invoked automatically upon {@link Lock#unlock()}.
	 * <p>
	 * NOTE: The returned lock does not support {@link #newCondition()}.
	 * 
	 * @param globalName
	 *            a name that uniquely identifies the locked object across all the instances
	 * @param disposeWhenUnlocked
	 *            if true, disposes the returned lock automatically upon {@link Lock#unlock()}
	 * @param namespace
	 *            if not null, uses a namespaced Memcache service, which is also expected to be identical in all instances targeting a given
	 *            locked object. The globalName of the lock is in fact unique only within its namespace, so if you have different key sets
	 *            (e.g. stored in different tables), it can be handy to store each set in a different namespace corresponding to the table
	 *            (alternatively you can concatenate the key with the table name to achieve similar effect). However, if you're using
	 *            namespaces elsewhere in your app-engine project, it is highly recommended that you explicitly specify one here as well
	 * @param locallyFair
	 *            indicates whether it's important to treat the local threads waiting on the lock fairly
	 * @param sleepBetweenRetriesMS
	 *            delay between attempts to acquire the Memcache lock
	 * @return
	 */
	public static GaeMemcacheLock getLock(final String globalName, final boolean disposeWhenUnlocked, final String namespace,
			final boolean locallyFair, final long sleepBetweenRetriesMS)
	{
		try
		{
			return locks.acquire(globalName, new AutoEvictMap.Factory<String, GaeMemcacheLock>()
			{
				@Override
				public GaeMemcacheLock valueOf(String key)
				{
					return new GaeMemcacheLock(globalName, disposeWhenUnlocked, namespace, locallyFair, sleepBetweenRetriesMS);
				}
			});
		}
		catch (InterruptedException e)
		{
			throw new RuntimeException(e);
		}
	}

	public static void disposeLock(String globalName)
	{
		locks.release(globalName);
	}
}
