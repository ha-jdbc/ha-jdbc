/*
 * Copyright (c) 2004-2007, Identity Theft 911, LLC.  All rights reserved.
 */
package net.sf.hajdbc.local;

import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;

/**
 * @author Paul Ferraro
 */
public class SemaphoreReadWriteLock implements ReadWriteLock
{
	private static final int TOTAL_PERMITS = Integer.MAX_VALUE;
	
	Semaphore semaphore = new Semaphore(TOTAL_PERMITS, true);
	private Lock readLock = new SemaphoreLock(1);
	private Lock writeLock = new SemaphoreLock(TOTAL_PERMITS);
	
	/**
	 * @see java.util.concurrent.locks.ReadWriteLock#readLock()
	 */
	@Override
	public Lock readLock()
	{
		return this.readLock;
	}

	/**
	 * @see java.util.concurrent.locks.ReadWriteLock#writeLock()
	 */
	@Override
	public Lock writeLock()
	{
		return this.writeLock;
	}

	private class SemaphoreLock implements Lock
	{
		private int permits;
		
		public SemaphoreLock(int permits)
		{
			this.permits = permits;
		}
		
		/**
		 * @see java.util.concurrent.locks.Lock#lock()
		 */
		@Override
		public void lock()
		{
			SemaphoreReadWriteLock.this.semaphore.acquireUninterruptibly(this.permits);
		}

		/**
		 * @see java.util.concurrent.locks.Lock#lockInterruptibly()
		 */
		@Override
		public void lockInterruptibly() throws InterruptedException
		{
			SemaphoreReadWriteLock.this.semaphore.acquire(this.permits);
		}

		/**
		 * @see java.util.concurrent.locks.Lock#newCondition()
		 */
		@Override
		public Condition newCondition()
		{
			throw new UnsupportedOperationException();
		}

		/**
		 * @see java.util.concurrent.locks.Lock#tryLock()
		 */
		@Override
		public boolean tryLock()
		{
			return SemaphoreReadWriteLock.this.semaphore.tryAcquire(this.permits);
		}

		/**
		 * @see java.util.concurrent.locks.Lock#tryLock(long, java.util.concurrent.TimeUnit)
		 */
		@Override
		public boolean tryLock(long time, TimeUnit unit) throws InterruptedException
		{
			return SemaphoreReadWriteLock.this.semaphore.tryAcquire(this.permits, time, unit);
		}

		/**
		 * @see java.util.concurrent.locks.Lock#unlock()
		 */
		@Override
		public void unlock()
		{
			SemaphoreReadWriteLock.this.semaphore.release(this.permits);
		}
	}
}
