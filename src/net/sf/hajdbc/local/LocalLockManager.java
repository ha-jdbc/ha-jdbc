/*
 * HA-JDBC: High-Availability JDBC
 * Copyright (c) 2004-2006 Paul Ferraro
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
import java.util.concurrent.locks.ReentrantReadWriteLock;

import net.sf.hajdbc.LockManager;

/**
 * @author Paul Ferraro
 */
public class LocalLockManager implements LockManager
{
	private Map<String, ReadWriteLock> lockMap = new HashMap<String, ReadWriteLock>();

	/**
	 * @see net.sf.hajdbc.LockManager#readLock(java.lang.String)
	 */
	public Lock readLock(String object)
	{
		Lock lock = this.getReadWriteLock(null).readLock();
		
		return (object == null) ? lock : new GlobalLock(lock, this.getReadWriteLock(object).readLock());
	}
	
	/**
	 * @see net.sf.hajdbc.LockManager#writeLock(java.lang.String)
	 */
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
			lock = createReadWriteLock();
			
			this.lockMap.put(object, lock);
		}
		
		return lock;
	}
	
	/**
	 * Work around for missing constructor in backport-util-concurrent package.
	 * @return ReadWriteLock implementation
	 */
	private ReadWriteLock createReadWriteLock()
	{
		try
		{
			return new ReentrantReadWriteLock(true);
		}
		catch (NoSuchMethodError e)
		{
			return new ReentrantReadWriteLock();
		}
	}
	
	private class GlobalLock implements Lock
	{
		private Lock globalLock;
		private Lock lock;
		
		public GlobalLock(Lock globalLock, Lock lock)
		{
			this.globalLock = globalLock;
			this.lock = lock;
		}
		
		public void lock()
		{
			this.globalLock.lock();
			this.lock.lock();
		}

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

		public void unlock()
		{
			this.lock.unlock();
			this.globalLock.unlock();
		}

		public Condition newCondition()
		{
			throw new UnsupportedOperationException();
		}
	}

	/**
	 * @see net.sf.hajdbc.LockManager#start()
	 */
	public void start() throws Exception
	{
		// Do nothing
	}

	/**
	 * @see net.sf.hajdbc.LockManager#stop()
	 */
	public void stop()
	{
		// Do nothing
	}
}
