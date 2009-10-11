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

/**
 * An implementation of {@link java.util.concurrent.locks.Lock} using a binary semaphore.
 * Unlike the {@link java.util.concurrent.locks.ReentrantLock} this lock can be locked and unlocked by different threads.
 * Conditions are not supported.
 * 
 * @author Paul Ferraro
 */
public class SemaphoreLock implements Lock
{
	private final Semaphore semaphore;
	
	public SemaphoreLock(boolean fair)
	{
		this(new Semaphore(1, fair));
	}
	
	SemaphoreLock(Semaphore semaphore)
	{
		this.semaphore = semaphore;
	}
	
	/**
	 * @see java.util.concurrent.locks.Lock#lock()
	 */
	@Override
	public void lock()
	{
		this.semaphore.acquireUninterruptibly();
	}

	/**
	 * @see java.util.concurrent.locks.Lock#lockInterruptibly()
	 */
	@Override
	public void lockInterruptibly() throws InterruptedException
	{
		this.semaphore.acquire();
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
		return this.semaphore.tryAcquire();
	}

	/**
	 * @see java.util.concurrent.locks.Lock#tryLock(long, java.util.concurrent.TimeUnit)
	 */
	@Override
	public boolean tryLock(long time, TimeUnit unit) throws InterruptedException
	{
		return this.semaphore.tryAcquire(time, unit);
	}

	/**
	 * @see java.util.concurrent.locks.Lock#unlock()
	 */
	@Override
	public void unlock()
	{
		this.semaphore.release();
	}
}
