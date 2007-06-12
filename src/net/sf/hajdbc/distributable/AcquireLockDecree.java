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
package net.sf.hajdbc.distributable;

import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;

import org.jgroups.Address;

import net.sf.hajdbc.LockManager;

/**
 * @author Paul Ferraro
 * @since 2.0
 */
public class AcquireLockDecree extends AbstractLockDecree
{
	private static final long serialVersionUID = 9016576896686385467L;

	public AcquireLockDecree()
	{
		super();
	}

	public AcquireLockDecree(String id, Address address)
	{
		super(id, address);
	}

	/**
	 * @see net.sf.hajdbc.distributable.LockDecree#vote(net.sf.hajdbc.LockManager, java.util.Map)
	 */
	@Override
	public boolean vote(LockManager lockManager, Map<LockDecree, Lock> lockMap)
	{
		Lock lock = new DistributableLockAdapter(lockManager.writeLock(this.getId()));
		
		boolean locked = lock.tryLock();
		
		if (locked)
		{
			synchronized (lockMap)
			{
				lockMap.put(this, lock);
			}
		}
		
		return locked;
	}
	
	/**
	 * Adapts a lock so that it can be manipulated by different threads.
	 */
	private class DistributableLockAdapter implements Lock
	{
		private LockThread thread;
		
		public DistributableLockAdapter(Lock lock)
		{
			this.thread = new LockThread(lock);
			this.thread.setDaemon(true);
		}
		
		/**
		 * @see java.util.concurrent.locks.Lock#lock()
		 */
		@Override
		public void lock()
		{
			LockMethod method = new LockMethod()
			{
				@Override
				public boolean lock(Lock lock)
				{
					lock.lock();
					
					return true;
				}
			};
			
			this.thread.setMethod(method);
			this.thread.start();
			
			synchronized (this.thread)
			{
				while (!this.thread.isReady())
				{
					try
					{
						this.thread.wait();
					}
					catch (InterruptedException e)
					{
						// Ignore
					}
				}
			}
		}

		/**
		 * @see java.util.concurrent.locks.Lock#lockInterruptibly()
		 */
		@Override
		public void lockInterruptibly() throws InterruptedException
		{
			LockMethod method = new LockMethod()
			{
				@Override
				public boolean lock(Lock lock) throws InterruptedException
				{
					lock.lockInterruptibly();
					
					return true;
				}
			};
			
			this.thread.setMethod(method);
			this.thread.start();
			
			synchronized (this.thread)
			{
				while (!this.thread.isReady())
				{
					try
					{
						this.thread.wait();
					}
					catch (InterruptedException e)
					{
						this.thread.interrupt();
						
						throw e;
					}
				}
			}
		}

		/**
		 * @see java.util.concurrent.locks.Lock#tryLock()
		 */
		@Override
		public boolean tryLock()
		{
			LockMethod method = new LockMethod()
			{
				@Override
				public boolean lock(Lock lock)
				{
					return lock.tryLock();
				}
			};
			
			this.thread.setMethod(method);
			this.thread.start();
			
			synchronized (this.thread)
			{
				while (!this.thread.isReady())
				{
					try
					{
						this.thread.wait();
					}
					catch (InterruptedException e)
					{
						// Ignore
					}
				}
			}

			return this.thread.isLocked();
		}

		/**
		 * @see java.util.concurrent.locks.Lock#tryLock(long, java.util.concurrent.TimeUnit)
		 */
		@Override
		public boolean tryLock(final long time, final TimeUnit unit) throws InterruptedException
		{
			LockMethod method = new LockMethod()
			{
				@Override
				public boolean lock(Lock lock) throws InterruptedException
				{
					return lock.tryLock(time, unit);
				}
			};
			
			this.thread.setMethod(method);
			this.thread.start();
			
			synchronized (this.thread)
			{
				while (!this.thread.isReady())
				{
					try
					{
						this.thread.wait();
					}
					catch (InterruptedException e)
					{
						this.thread.interrupt();
						
						throw e;
					}
				}
			}
			
			return this.thread.isLocked();
		}

		/**
		 * @see java.util.concurrent.locks.Lock#unlock()
		 */
		@Override
		public void unlock()
		{
			if (this.thread.isLocked())
			{
				this.thread.interrupt();
			}
		}

		/**
		 * @see java.util.concurrent.locks.Lock#newCondition()
		 */
		@Override
		public Condition newCondition()
		{
			return null;
		}
		
		/**
		 * Thread that locks the specified lock upon starting.
		 * Lock is unlocked by interrupting the running thread.
		 * Caller must call setMethod(...) before starting.
		 */
		private class LockThread extends Thread
		{
			private Lock lock;
			private LockMethod method;
			private boolean ready = false;
			private boolean locked = false;
			
			public LockThread(Lock lock)
			{
				super();
				
				this.lock = lock;
			}
			
			public void setMethod(LockMethod method)
			{
				this.method = method;
			}
			
			public boolean isLocked()
			{
				return this.locked;
			}
			
			public boolean isReady()
			{
				return this.ready;
			}
			
			/**
			 * @see java.lang.Runnable#run()
			 */
			@Override
			public synchronized void run()
			{
				try
				{
					this.locked = this.method.lock(this.lock);
				}
				catch (InterruptedException e)
				{
					this.interrupt();
				}
				
				this.ready = true;
				
				this.notify();
				
				if (this.locked)
				{
					// Wait for interrupt
					while (!this.isInterrupted())
					{
						try
						{
							this.wait();
						}
						catch (InterruptedException e)
						{
							this.interrupt();
						}
					}
					
					this.lock.unlock();
					this.locked = false;
				}
			}
		}
	}
	
	private interface LockMethod
	{
		public boolean lock(Lock lock) throws InterruptedException;
	}
}