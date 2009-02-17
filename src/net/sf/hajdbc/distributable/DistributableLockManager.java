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

import java.lang.reflect.Method;
import java.text.MessageFormat;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;

import net.sf.hajdbc.DatabaseCluster;
import net.sf.hajdbc.LockManager;

import org.jgroups.Address;
import org.jgroups.Message;
import org.jgroups.MessageListener;
import org.jgroups.blocks.GroupRequest;
import org.jgroups.blocks.MethodCall;
import org.jgroups.blocks.RpcDispatcher;
import org.jgroups.util.Rsp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * LockManager implementation that leverages a JGroups 2-phase voting adapter for obtain remote write locks.
 * 
 * @author Paul Ferraro
 */
public class DistributableLockManager extends AbstractMembershipListener implements LockManager, MessageListener
{
	private static final String CHANNEL = "{0}-lock"; //$NON-NLS-1$
	
	static Logger logger = LoggerFactory.getLogger(DistributableLockManager.class);
	
	protected RpcDispatcher dispatcher;
	protected int timeout;
	private LockManager lockManager;
	private Map<Address, Map<String, Lock>> addressMap = new ConcurrentHashMap<Address, Map<String, Lock>>();
	
	/**
	 * Constructs a new DistributableLock.
	 * @param <D> either java.sql.Driver or javax.sql.Datasource
	 * @param databaseCluster a database cluster
	 * @param decorator a decorator
	 * @throws Exception
	 */
	public <D> DistributableLockManager(DatabaseCluster<D> databaseCluster, DistributableDatabaseClusterDecorator decorator) throws Exception
	{
		super(decorator.createChannel(MessageFormat.format(CHANNEL, databaseCluster.getId())));
		
		this.lockManager = databaseCluster.getLockManager();
		
		this.timeout = decorator.getTimeout();

		this.dispatcher = new RpcDispatcher(this.channel, this, this, this);
	}

	/**
	 * @see net.sf.hajdbc.Lifecycle#start()
	 */
	@Override
	public void start() throws Exception
	{
		this.channel.connect(this.channel.getClusterName());
		
		this.lockManager.start();
	}
	
	/**
	 * @see net.sf.hajdbc.Lifecycle#stop()
	 */
	@Override
	public void stop()
	{
		this.channel.close();

		this.lockManager.stop();
	}
	
	/**
	 * Read locks are local.
	 * @see net.sf.hajdbc.LockManager#readLock(java.lang.String)
	 */
	@Override
	public Lock readLock(String object)
	{
		return this.lockManager.readLock(object);
	}

	/**
	 * Write locks are distributed.
	 * @see net.sf.hajdbc.LockManager#writeLock(java.lang.String)
	 */
	@Override
	public Lock writeLock(String object)
	{
		return new DistributableLock(object, this.lockManager.writeLock(object));
	}

	/**
	 * Votes on the specified decree.
	 * @param decree a lock decree
	 * @return true, if success, false if failure
	 */
	public boolean vote(LockDecree decree)
	{
		Map<String, Lock> lockMap = this.addressMap.get(decree.getAddress());
		
		// Vote negatively for decrees from non-members
		if (lockMap == null)
		{
			return false;
		}
		
		return decree.vote(this.lockManager, lockMap);
	}
	
	/**
	 * @see net.sf.hajdbc.distributable.AbstractMembershipListener#memberJoined(org.jgroups.Address)
	 */
	@Override
	protected void memberJoined(Address address)
	{
		this.addressMap.put(address, new HashMap<String, Lock>());
	}

	/**
	 * @see net.sf.hajdbc.distributable.AbstractMembershipListener#memberLeft(org.jgroups.Address)
	 */
	@Override
	protected void memberLeft(Address address)
	{
		Map<String, Lock> lockMap = this.addressMap.remove(address);
		
		for (Lock lock: lockMap.values())
		{
			lock.unlock();
		}
	}

	/**
	 * @see org.jgroups.MessageListener#getState()
	 */
	@Override
	public byte[] getState()
	{
		return null;
	}

	/**
	 * @see org.jgroups.MessageListener#receive(org.jgroups.Message)
	 */
	@Override
	public void receive(Message message)
	{
		// Do nothing
	}

	/**
	 * @see org.jgroups.MessageListener#setState(byte[])
	 */
	@Override
	public void setState(byte[] arg0)
	{
		// Do nothing
	}

	private class DistributableLock implements Lock
	{
		private LockDecree acquireDecree;
		private LockDecree releaseDecree;
		private Lock lock;
		
		/**
		 * @param object
		 * @param lock
		 */
		public DistributableLock(String object, Lock lock)
		{
			Address address = DistributableLockManager.this.channel.getLocalAddress();
			
			this.acquireDecree = new AcquireLockDecree(object, address);
			this.releaseDecree = new ReleaseLockDecree(object, address);
			
			this.lock = lock;
		}
		
		/**
		 * @see java.util.concurrent.locks.Lock#lock()
		 */
		@Override
		public void lock()
		{
			while (!DistributableLockManager.this.isMembershipEmpty())
			{
				if (this.tryLockFairly()) return;
				
				Thread.yield();
			}
			
			this.lock.lock();
		}

		/**
		 * @see java.util.concurrent.locks.Lock#lockInterruptibly()
		 */
		@Override
		public void lockInterruptibly() throws InterruptedException
		{
			while (!DistributableLockManager.this.isMembershipEmpty())
			{
				if (this.tryLockFairly()) return;

				if (Thread.currentThread().isInterrupted())
				{
					throw new InterruptedException();
				}
				
				Thread.yield();
			}
			
			this.lock.lockInterruptibly();
		}

		/**
		 * @see java.util.concurrent.locks.Lock#tryLock()
		 */
		@Override
		public boolean tryLock()
		{
			if (this.lock.tryLock())
			{
				if (this.tryRemoteLock())
				{
					return true;
				}
				
				this.lock.unlock();
			}
			
			return false;
		}

		/**
		 * Like {@link #tryLock()}, but do not barge on other waiting threads
		 * @return true, if lock acquired, false otherwise
		 * @throws InterruptedException
		 */
		private boolean tryLockFairly()
		{
			try
			{
				if (this.lock.tryLock(0, TimeUnit.SECONDS))
				{
					if (this.tryRemoteLock())
					{
						return true;
					}
					
					this.lock.unlock();
				}
			}
			catch (InterruptedException e)
			{
				Thread.currentThread().interrupt();
			}
			
			return false;
		}
		
		/**
		 * @see java.util.concurrent.locks.Lock#tryLock(long, java.util.concurrent.TimeUnit)
		 */
		@Override
		public boolean tryLock(long timeout, TimeUnit unit) throws InterruptedException
		{
			// Convert timeout to milliseconds
			long ms = unit.toMillis(timeout);
			
			long stopTime = System.currentTimeMillis() + ms;
			
			do
			{
				if (DistributableLockManager.this.isMembershipEmpty())
				{
					return this.lock.tryLock(ms, TimeUnit.MILLISECONDS);
				}
				
				if (this.tryLockFairly())
				{
					return true;
				}

				if (Thread.currentThread().isInterrupted())
				{
					throw new InterruptedException();
				}
				
				ms = stopTime - System.currentTimeMillis();
			}
			while (ms >= 0);
			
			return false;
		}

		/**
		 * @see java.util.concurrent.locks.Lock#unlock()
		 */
		@Override
		public void unlock()
		{
			this.remoteUnlock();

			this.lock.unlock();
		}
		
		private boolean tryRemoteLock()
		{
			Map<Boolean, Vector<Address>> map = null;
			
			try
			{
				map = this.remoteLock();
				
				return map.get(false).isEmpty();
			}
			finally
			{
				if (map != null)
				{
					this.remoteUnlock(map.get(true));
				}
			}
		}
		
		private Map<Boolean, Vector<Address>> remoteLock()
		{
			return DistributableLockManager.this.remoteVote(this.acquireDecree, null, DistributableLockManager.this.timeout);
		}

		private Map<Boolean, Vector<Address>> remoteUnlock()
		{
			return this.remoteUnlock(null);
		}
		
		private Map<Boolean, Vector<Address>> remoteUnlock(Vector<Address> address)
		{
			return DistributableLockManager.this.remoteVote(this.releaseDecree, address, 0);
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
	
	Map<Boolean, Vector<Address>> remoteVote(LockDecree decree, Vector<Address> addresses, long timeout)
	{
		Map<Boolean, Vector<Address>> map = new TreeMap<Boolean, Vector<Address>>();
		
		int size = (addresses != null) ? addresses.size() : this.getMembershipSize();
		
		map.put(true, new Vector<Address>(size));
		map.put(false, new Vector<Address>(size));
		
		if (size > 0)
		{
			try
			{
				Method method = this.getClass().getMethod("vote", LockDecree.class); //$NON-NLS-1$
				
				MethodCall call = new MethodCall(method, new Object[] { decree });
				
				Collection<Rsp> responses = this.dispatcher.callRemoteMethods(addresses, call, GroupRequest.GET_ALL, timeout).values();
				
				for (Rsp response: responses)
				{
					Object value = response.wasReceived() ? response.getValue() : false;
					
					map.get((value != null) ? value : false).add(response.getSender());
				}
			}
			catch (NoSuchMethodException e)
			{
				throw new IllegalStateException(e);
			}
		}
		
		return map;
	}
}