/*
 * HA-JDBC: High-Availability JDBC
 * Copyright 2004-2009 Paul Ferraro
 * 
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 * 
 * You should have received a copy of the GNU Lesser General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package net.sf.hajdbc.lock.semaphore;

import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;


/**
 * Simple {@link java.util.concurrent.locks.ReadWriteLock} implementation that uses a semaphore.
 * A read lock requires 1 permit, while a write lock requires all the permits.
 * Lock upgrading and downgrading is not supported; nor are conditions.
 * 
 * @author Paul Ferraro
 */
public class SemaphoreReadWriteLock implements ReadWriteLock
{
	private static final int DEFAULT_PERMITS = Integer.MAX_VALUE;
	
	private final Lock readLock;
	private final Lock writeLock;
	
	public SemaphoreReadWriteLock(boolean fair)
	{
		this(DEFAULT_PERMITS, fair);
	}

	public SemaphoreReadWriteLock(int permits, boolean fair)
	{
		Semaphore semaphore = new Semaphore(permits, fair);
		
		this.readLock = new SemaphoreLock(semaphore);
		this.writeLock = new SemaphoreWriteLock(semaphore, permits);
	}
	
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
	
	private static class SemaphoreWriteLock implements Lock
	{
		private final Semaphore semaphore;
		private final int permits;
		
		SemaphoreWriteLock(Semaphore semaphore, int permits)
		{
			this.semaphore = semaphore;
			this.permits = permits;
		}
		
		/**
		 * Helps avoid write lock starvation, when using an unfair acquisition policy by draining all available permits.
		 * @return the number of drained permits
		 */
		private int drainPermits()
		{
			return this.semaphore.isFair() ? 0 : this.semaphore.drainPermits();
		}
		
		/**
		 * @see java.util.concurrent.locks.Lock#lock()
		 */
		@Override
		public void lock()
		{
			int drained = this.drainPermits();
			
			if (drained < this.permits)
			{
				this.semaphore.acquireUninterruptibly(this.permits - drained);
			}
		}

		/**
		 * @see java.util.concurrent.locks.Lock#lockInterruptibly()
		 */
		@Override
		public void lockInterruptibly() throws InterruptedException
		{
			int drained = this.drainPermits();
			
			if (drained < this.permits)
			{
				try
				{
					this.semaphore.acquire(this.permits - drained);
				}
				catch (InterruptedException e)
				{
					if (drained > 0)
					{
						this.semaphore.release(drained);
					}
					
					throw e;
				}
			}
		}

		/**
		 * @see java.util.concurrent.locks.Lock#tryLock()
		 */
		@Override
		public boolean tryLock()
		{
			// This will barge the fairness queue, so there's no need to drain permits
			return this.semaphore.tryAcquire(this.permits);
		}

		/**
		 * @see java.util.concurrent.locks.Lock#tryLock(long, java.util.concurrent.TimeUnit)
		 */
		@Override
		public boolean tryLock(long timeout, TimeUnit unit) throws InterruptedException
		{
			int drained = this.drainPermits();
			
			if (drained == this.permits) return true;
			
			boolean acquired = false;
			
			try
			{
				acquired = this.semaphore.tryAcquire(this.permits - drained, timeout, unit);
			}
			finally
			{
				if (!acquired && (drained > 0))
				{
					this.semaphore.release(drained);
				}
			}
			
			return acquired;
		}

		/**
		 * @see java.util.concurrent.locks.Lock#unlock()
		 */
		@Override
		public void unlock()
		{
			this.semaphore.release(this.permits);
		}
		
		/**
		 * @see java.util.concurrent.locks.Lock#newCondition()
		 */
		@Override
		public Condition newCondition()
		{
			throw new UnsupportedOperationException();
		}
	}
}
