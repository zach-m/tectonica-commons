package com.tectonica.gae;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.google.appengine.api.memcache.Expiration;
import com.google.appengine.api.memcache.MemcacheService;
import com.google.appengine.api.memcache.MemcacheService.SetPolicy;
import com.google.appengine.api.memcache.MemcacheServiceFactory;

/**
 * a {@link Lock} implementation for Google App Engine applications, which uses the Memcache service to ensure global locking (i.e. among
 * all the instances of the application). The lock is re-entrant (i.e. can be locked and then re-locked in a nested method by the same
 * thread) and minimizes the amount of calls to the Memcache service by having all threads from a single instance share the Memcache
 * polling loop.
 * <p>
 * Locking with Memcache is not a bullet-proof solution, as unexpected eviction of the cache may result it a situation where one instance
 * acquires a lock that is in fact taken by another. However, the risk is very minimal especially if using the Dedicated Plan from Google
 * (see <a href='https://cloud.google.com/appengine/docs/adminconsole/memcache'>here</a>). Also, a lock entry in Memcache is very unlikely
 * to be evicted due to LRU considerations, as locks are either short-lived or very frequently updated (in high contention).
 * 
 * @author Zach Melamed
 */
public class GaeMemcacheLock implements Lock
{
	private static final int LOCK_AUTO_EXPIRATION_MS = 30000;
	private static final long SLEEP_BETWEEN_RETRIES_MS = 50L;

	private final String globalName;
	private final long sleepBetweenRetriesMS;
	private final ReentrantLock localLock;
	private ThreadLocal<Integer> globalHoldCount = new ThreadLocal<Integer>()
	{
		@Override
		protected Integer initialValue()
		{
			return 0;
		}
	};

	public GaeMemcacheLock(String globalName)
	{
		this(globalName, false, SLEEP_BETWEEN_RETRIES_MS);
	}

	public GaeMemcacheLock(String globalName, boolean locallyFair, long sleepBetweenRetriesMS)
	{
		this.globalName = globalName;
		this.sleepBetweenRetriesMS = sleepBetweenRetriesMS;
		localLock = new ReentrantLock(locallyFair);
	}

	@Override
	public void lockInterruptibly() throws InterruptedException
	{
		localLock.lock();
		waitForGlobalLock(null);
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
	}

	@Override
	public boolean tryLock()
	{
		if (!localLock.tryLock())
			return false;
		if (!lockGlobally())
		{
			localLock.unlock();
			return false;
		}
		return true;
	}

	@Override
	public boolean tryLock(long time, TimeUnit unit) throws InterruptedException
	{
		long timeout = System.currentTimeMillis() + unit.toMillis(time);
		if (localLock.tryLock(time, unit))
			return waitForGlobalLock(timeout);
		return false;
	}

	@Override
	public Condition newCondition()
	{
		throw new UnsupportedOperationException();
	}

	/**
	 * this method blocks execution until it acquires the global lock (or gets interrupted). it does so using an infinite loop which checks
	 * a shared Memcache entry once in every short-while, until it succeeds in acquiring the lock
	 */
	private boolean waitForGlobalLock(Long timeout) throws InterruptedException
	{
		while (true)
		{
			long before = System.currentTimeMillis();

			if (lockGlobally())
				return true; // don't wait if we got the lock

			// check for timeout
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
	 * guaranteed to run only after acquiring the local lock, this method attempts to acquire the global lock too. it uses a reference
	 * counting methodology to support reentrancy.
	 */
	private boolean lockGlobally()
	{
		int holdCount = globalHoldCount.get().intValue();

		boolean acquired;
		if (holdCount > 0)
			acquired = true;
		else
			acquired = mc().put(globalName, "X", Expiration.byDeltaMillis(LOCK_AUTO_EXPIRATION_MS), SetPolicy.ADD_ONLY_IF_NOT_PRESENT);

		if (acquired)
			globalHoldCount.set(holdCount + 1);

		return acquired;
	}

	/**
	 * guaranteed to run before releasing the local lock, this releases the global lock first.it uses a reference counting methodology to
	 * support reentrancy
	 */
	private void unlockGlobally()
	{
		int holdCount = globalHoldCount.get().intValue() - 1;
		globalHoldCount.set(holdCount);
		if (holdCount == 0)
			mc().delete(globalName);
	}

	/**
	 * convenience method
	 */
	private final static MemcacheService mc()
	{
		return MemcacheServiceFactory.getMemcacheService();
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
}
