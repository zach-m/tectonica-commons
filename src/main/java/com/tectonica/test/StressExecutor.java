package com.tectonica.test;

import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RecursiveAction;
import java.util.concurrent.atomic.AtomicInteger;

public class StressExecutor extends RecursiveAction
{
	private static final long serialVersionUID = 1L;

	public static abstract class StressRunnable
	{
		public abstract void run(int index, int threadNo);

		public void preSlice(int from, int to, int threadNo)
		{}

		public void postSlice(int from, int to, int threadNo)
		{}
	}

	private static class GlobalParams
	{
		private final int maxThreads;
		private final int maxSliceSize;
		private final StressRunnable runnable;

		public GlobalParams(int maxThreads, int maxSliceSize, StressRunnable runnable)
		{
			this.maxThreads = maxThreads;
			this.maxSliceSize = maxSliceSize;
			this.runnable = runnable;
		}

		private AtomicInteger seq = new AtomicInteger(0);

		public int nextSeq()
		{
			return seq.incrementAndGet();
		}
	}

	private static class SliceParams
	{
		private final int from;
		private final int to;

		public SliceParams(int from, int to)
		{
			this.from = from;
			this.to = to;
		}
	}

	private GlobalParams config;
	private SliceParams slice;

	/**
	 * external constructor for invocation by the user
	 */
	public StressExecutor(int from, int to, int maxThreads, int maxSliceSize, StressRunnable runnable)
	{
		slice = new SliceParams(from, to);
		config = new GlobalParams(maxThreads, maxSliceSize, runnable);
	}

	/**
	 * internal constructor for the forks
	 */
	private StressExecutor(SliceParams slice, GlobalParams config)
	{
		this.slice = slice;
		this.config = config;
	}

	public void execute()
	{
		ForkJoinPool pool = new ForkJoinPool(config.maxThreads);
		pool.invoke(this);
	}

	@Override
	protected void compute()
	{
		int length = slice.to - slice.from;
		if (length <= config.maxSliceSize)
		{
			computeSlice(config.nextSeq());
		}
		else
		{
			int mid = slice.from + (length / 2);
			StressExecutor leftSlice = new StressExecutor(new SliceParams(slice.from, mid), config);
			StressExecutor rightSlice = new StressExecutor(new SliceParams(mid, slice.to), config);
			invokeAll(leftSlice, rightSlice);
		}
	}

	private void computeSlice(int threadNo)
	{
		int from = slice.from;
		int to = slice.to;
		StressRunnable runnable = config.runnable;

		runnable.preSlice(from, to, threadNo);
		for (int index = from; index < to; index++)
			runnable.run(index, threadNo);
		runnable.postSlice(from, to, threadNo);
	}
}
