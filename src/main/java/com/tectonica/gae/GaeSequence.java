package com.tectonica.gae;

import java.util.ConcurrentModificationException;
import java.util.Random;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.appengine.api.NamespaceManager;
import com.google.appengine.api.datastore.DatastoreService;
import com.google.appengine.api.datastore.DatastoreServiceFactory;
import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.EntityNotFoundException;
import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.KeyFactory;
import com.google.appengine.api.datastore.Query;
import com.google.appengine.api.datastore.Transaction;
import com.google.appengine.api.memcache.Expiration;
import com.google.appengine.api.memcache.MemcacheService;
import com.google.appengine.api.memcache.MemcacheService.SetPolicy;
import com.google.appengine.api.memcache.MemcacheServiceFactory;

/**
 * Slightly modified version of <a href='https://github.com/GoogleCloudPlatform/appengine-sharded-counters-java'>Google's suggested
 * implementation</a> for counters. Explanation can be found <a
 * href='https://cloud.google.com/appengine/articles/sharding_counters'>here</a>.
 * <p>
 * This is an implementation of a counter which can be incremented rapidly.
 * 
 * Capable of incrementing the counter and increasing the number of shards. When incrementing, a random shard is selected to prevent a
 * single shard from being written too frequently. If increments are being made too quickly, increase the number of shards to divide the
 * load. Performs datastore operations using the low level datastore API.
 */
public class GaeSequence
{
	/**
	 * Entity kind representing a named sharded counter.
	 */
	private static final String KIND = "Counter";

	/**
	 * Property to store the number of shards in a given {@value #KIND} named sharded counter.
	 */
	private static final String SHARD_COUNT = "shard_count";

	/**
	 * Entity kind prefix, which is concatenated with the counter name to form the final entity kind, which represents counter shards.
	 */
	private static final String KIND_PREFIX = "CounterShard_";

	/**
	 * Property to store the current count within a counter shard.
	 */
	private static final String COUNT = "count";

	/**
	 * DatastoreService object for Datastore access.
	 */
	private static final DatastoreService DS = DatastoreServiceFactory.getDatastoreService();

	/**
	 * Default number of shards.
	 */
	private static final int INITIAL_SHARDS = 5;

	/**
	 * Cache duration for memcache.
	 */
	private static final int CACHE_PERIOD = 60;

	/**
	 * The name of this counter.
	 */
	private final String counterName;

	private final String namespace;

	/**
	 * A random number generating, for distributing writes across shards.
	 */
	private final Random generator = new Random();

	/**
	 * The counter shard kind for this counter.
	 */
	private String kind;

	/**
	 * Memcache service object for Memcache access.
	 */
	private final MemcacheService mc = MemcacheServiceFactory.getMemcacheService();

	/**
	 * A logger object.
	 */
	private static final Logger LOG = Logger.getLogger(GaeSequence.class.getName());

	public GaeSequence(final String name)
	{
		this(name, null);
	}

	/**
	 * Constructor which creates a sharded counter using the provided counter name
	 * 
	 * @param name
	 *            name of the sharded counter
	 */
	public GaeSequence(final String name, String namespace)
	{
		counterName = name;
		kind = KIND_PREFIX + counterName;
		this.namespace = namespace;
	}

	private Key counterKey()
	{
		NamespaceManager.set(namespace);
		return KeyFactory.createKey(KIND, counterName);
	}

	/**
	 * Increase the number of shards for a given sharded counter. Will never decrease the number of shards.
	 * 
	 * @param count
	 *            Number of new shards to build and store
	 */
	public final void addShards(final int count)
	{
		incrementPropertyTx(counterKey(), SHARD_COUNT, count, INITIAL_SHARDS + count);
	}

	/**
	 * Retrieve the value of this sharded counter.
	 * 
	 * @return Summed total of all shards' counts
	 */
	public final long getCount()
	{
		Long value = (Long) mc.get(kind);
		if (value != null)
			return value;

		long sum = 0;
		NamespaceManager.set(namespace);
		Query query = new Query(kind);
		for (Entity shard : DS.prepare(query).asIterable())
			sum += (Long) shard.getProperty(COUNT);

		mc.put(kind, sum, Expiration.byDeltaSeconds(CACHE_PERIOD), SetPolicy.ADD_ONLY_IF_NOT_PRESENT);

		return sum;
	}

	public final long incrementAndGet()
	{
		increment();
		return getCount();
	}

	/**
	 * Increment the value of this sharded counter.
	 */
	public final void increment()
	{
		// Find how many shards are in this counter.
		int numShards = getShardCount();

		// Choose the shard randomly from the available shards.
		long shardNum = generator.nextInt(numShards);

		Key shardKey = KeyFactory.createKey(kind, Long.toString(shardNum));
		incrementPropertyTx(shardKey, COUNT, 1, 1);
		mc.increment(kind, 1);
	}

	/**
	 * Get the number of shards in this counter.
	 * 
	 * @return shard count
	 */
	private int getShardCount()
	{
		try
		{
			Entity counter = DS.get(counterKey());
			Long shardCount = (Long) counter.getProperty(SHARD_COUNT);
			return shardCount.intValue();
		}
		catch (EntityNotFoundException ignore)
		{
			return INITIAL_SHARDS;
		}
	}

	/**
	 * Increment datastore property value inside a transaction. If the entity
	 * with the provided key does not exist, instead create an entity with the
	 * supplied initial property value.
	 * 
	 * @param key
	 *            the entity key to update or create
	 * @param prop
	 *            the property name to be incremented
	 * @param increment
	 *            the amount by which to increment
	 * @param initialValue
	 *            the value to use if the entity does not exist
	 */
	private void incrementPropertyTx(final Key key, final String prop, final long increment, final long initialValue)
	{
		Transaction tx = DS.beginTransaction();
		Entity thing;
		long value;
		try
		{
			try
			{
				thing = DS.get(tx, key);
				value = (Long) thing.getProperty(prop) + increment;
			}
			catch (EntityNotFoundException e)
			{
				thing = new Entity(key);
				value = initialValue;
			}
			thing.setUnindexedProperty(prop, value);
			DS.put(tx, thing);
			tx.commit();
		}
		catch (ConcurrentModificationException e)
		{
			LOG.log(Level.WARNING, "You may need more shards. Consider adding more shards.");
			LOG.log(Level.WARNING, e.toString(), e);
		}
		catch (Exception e)
		{
			LOG.log(Level.WARNING, e.toString(), e);
		}
		finally
		{
			if (tx.isActive())
				tx.rollback();
		}
	}
}
