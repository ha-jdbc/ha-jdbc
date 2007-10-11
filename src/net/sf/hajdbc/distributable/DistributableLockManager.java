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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;

import net.sf.hajdbc.DatabaseCluster;
import net.sf.hajdbc.LockManager;
import net.sf.hajdbc.util.Strings;

import org.jgroups.Address;
import org.jgroups.ChannelException;
import org.jgroups.blocks.VoteException;
import org.jgroups.blocks.VotingAdapter;
import org.jgroups.blocks.VotingListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * LockManager implementation that leverages a JGroups 2-phase voting adapter for obtain remote write locks.
 * 
 * @author Paul Ferraro
 */
public class DistributableLockManager extends AbstractMembershipListener implements LockManager, VotingListener
{
	static Logger logger = LoggerFactory.getLogger(DistributableLockManager.class);
	
	protected VotingAdapter votingAdapter;
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
		super(decorator.createChannel(databaseCluster.getId() + "-lock")); //$NON-NLS-1$
		
		this.lockManager = databaseCluster.getLockManager();
		
		this.timeout = decorator.getTimeout();

		this.votingAdapter = new VotingAdapter(this.channel);

		this.votingAdapter.addVoteListener(this);
		this.votingAdapter.addMembershipListener(this);
	}

	@Override
	public void start() throws Exception
	{
		this.channel.connect(this.channel.getClusterName());
		
		this.lockManager.start();
	}
	
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
	 * @see org.jgroups.blocks.VotingListener#vote(java.lang.Object)
	 */
	@Override
	public boolean vote(Object object) throws VoteException
	{
		if ((object == null) || !(object instanceof LockDecree)) throw new VoteException(Strings.EMPTY);
		
		LockDecree decree = (LockDecree) object;
		
		return decree.vote(this.lockManager, this.addressMap.get(decree.getAddress()));
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

	private class DistributableLock implements Lock
	{
		private String object;
		private Lock lock;
		
		public DistributableLock(String object, Lock lock)
		{
			this.object = object;
			this.lock = lock;
		}
		
		/**
		 * @see java.util.concurrent.locks.Lock#lock()
		 */
		@Override
		public void lock()
		{
			while (!this.tryLock())
			{
				// Do nothing
			}
		}

		/**
		 * @see java.util.concurrent.locks.Lock#lockInterruptibly()
		 */
		@Override
		public void lockInterruptibly() throws InterruptedException
		{
			while (!this.tryLock())
			{
				if (Thread.currentThread().isInterrupted())
				{
					throw new InterruptedException();
				}
			}
		}

		/**
		 * @see java.util.concurrent.locks.Lock#tryLock()
		 */
		@Override
		public boolean tryLock()
		{
			if (!this.lock.tryLock()) return false;
			
			boolean locked = this.vote(new AcquireLockDecree(this.object, DistributableLockManager.this.channel.getLocalAddress()), DistributableLockManager.this.timeout);
			
			if (!locked)
			{
				this.unlock();
			}
			
			return locked;
		}

		/**
		 * @see java.util.concurrent.locks.Lock#tryLock(long, java.util.concurrent.TimeUnit)
		 */
		@Override
		public boolean tryLock(long timeout, TimeUnit unit)
		{
			long stopTime = System.currentTimeMillis() + unit.toMillis(timeout);
			
			while (!this.tryLock())
			{
				if (System.currentTimeMillis() >= stopTime)
				{
					return false;
				}
			}
			
			return true;
		}

		/**
		 * @see java.util.concurrent.locks.Lock#unlock()
		 */
		@Override
		public void unlock()
		{
			this.lock.unlock();
			
			this.vote(new ReleaseLockDecree(this.object, DistributableLockManager.this.channel.getLocalAddress()), 0);
		}

		private boolean vote(LockDecree decree, long timeout)
		{
			// Voting adapter return false if no members - so reverse this behavior
			if (DistributableLockManager.this.isMembershipEmpty()) return true;
			
			try
			{
				return DistributableLockManager.this.votingAdapter.vote(decree, timeout);
			}
			catch (ChannelException e)
			{
				throw new IllegalStateException(e);
			}
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