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
package net.sf.hajdbc.local;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;

import net.sf.hajdbc.LockManager;
import net.sf.hajdbc.util.concurrent.SemaphoreReadWriteLock;

/**
 * @author Paul Ferraro
 */
public class LocalLockManager implements LockManager
{
	private Map<String, ReadWriteLock> lockMap = new HashMap<String, ReadWriteLock>();

	/**
	 * @see net.sf.hajdbc.LockManager#readLock(java.lang.String)
	 */
	@Override
	public Lock readLock(String object)
	{
		Lock lock = this.getReadWriteLock(null).readLock();
		
		return (object == null) ? lock : new GlobalLock(lock, this.getReadWriteLock(object).readLock());
	}
	
	/**
	 * @see net.sf.hajdbc.LockManager#writeLock(java.lang.String)
	 */
	@Override
	public Lock writeLock(String object)
	{
		ReadWriteLock readWriteLock = this.getReadWriteLock(null);
		
		return (object == null) ? readWriteLock.writeLock() : new GlobalLock(readWriteLock.readLock(), this.getReadWriteLock(object).writeLock());
	}
	
	private synchronized ReadWriteLock getReadWriteLock(String object)
	{
		ReadWriteLock lock = this.lockMap.get(object);
		
		if (lock == null)
		{
			lock = new SemaphoreReadWriteLock();
			
			this.lockMap.put(object, lock);
		}
		
		return lock;
	}
	
	private static class GlobalLock implements Lock
	{
		private Lock globalLock;
		private Lock lock;
		
		GlobalLock(Lock globalLock, Lock lock)
		{
			this.globalLock = globalLock;
			this.lock = lock;
		}
		
		@Override
		public void lock()
		{
			this.globalLock.lock();
			this.lock.lock();
		}

		@Override
		public void lockInterruptibly() throws InterruptedException
		{
			this.globalLock.lockInterruptibly();
			
			try
			{
				this.lock.lockInterruptibly();
			}
			catch (InterruptedException e)
			{
				this.globalLock.unlock();
				throw e;
			}
		}

		@Override
		public boolean tryLock()
		{
			if (this.globalLock.tryLock())
			{
				if (this.lock.tryLock())
				{
					return true;
				}

				this.globalLock.unlock();
			}

			return false;
		}

		@Override
		public boolean tryLock(long time, TimeUnit unit) throws InterruptedException
		{
			if (this.globalLock.tryLock(time, unit))
			{
				if (this.lock.tryLock(time, unit))
				{
					return true;
				}

				this.globalLock.unlock();
			}

			return false;
		}

		@Override
		public void unlock()
		{
			this.lock.unlock();
			this.globalLock.unlock();
		}

		@Override
		public Condition newCondition()
		{
			throw new UnsupportedOperationException();
		}
	}

	/**
	 * @see net.sf.hajdbc.LockManager#start()
	 */
	@Override
	public void start() throws Exception
	{
		// Do nothing
	}

	/**
	 * @see net.sf.hajdbc.LockManager#stop()
	 */
	@Override
	public void stop()
	{
		// Do nothing
	}
}
