/*
 * HA-JDBC: High-Availability JDBC
 * Copyright (c) 2004-2007 Paul Ferraro
 * 
 * This library is free software; you can redistribute it and/or modify it 
 * under the terms of the GNU Lesser General Public License as published by the 
 * Free Software Foundation; either version 2.1 of the License, or (at your 
 * option) any later version.
 * 
 * This library is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or 
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public License 
 * for more details.
 * 
 * You should have received a copy of the GNU Lesser General Public License
 * along with this library; if not, write to the Free Software Foundation, 
 * Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA
 * 
 * Contact: ferraro@users.sourceforge.net
 */
package net.sf.hajdbc.util.concurrent;

import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;

/**
 * Simple {@link java.util.concurrent.lock.ReadWriteLock} implementation that uses a semaphore that grants up to {@link java.lang.Integer#MAX_VALUE} permits using a fair FIFO policy.
 * A read lock requires 1 permit, while a write lock requires all the permits.
 * Unlike the {@link java.util.concurrent.lock.ReentrantLock}, the read and write locks returned by this object can be locked and unlocked by different threads.
 * Lock upgrading and downgrading are not supported.  Conditions are also not supported.
 * 
 * @author Paul Ferraro
 */
public class SemaphoreReadWriteLock implements ReadWriteLock
{
	Semaphore semaphore = new Semaphore(Integer.MAX_VALUE, true);
	private Lock readLock = new SemaphoreLock(1);
	private Lock writeLock = new SemaphoreLock(Integer.MAX_VALUE);
	
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
