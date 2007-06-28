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
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;

import net.sf.hajdbc.DatabaseCluster;
import net.sf.hajdbc.LockManager;

import org.jgroups.Address;
import org.jgroups.Channel;
import org.jgroups.ChannelException;
import org.jgroups.MembershipListener;
import org.jgroups.View;
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
public class DistributableLockManager implements LockManager, VotingListener, MembershipListener
{
	static Logger logger = LoggerFactory.getLogger(DistributableLockManager.class);
	
	protected VotingAdapter votingAdapter;
	protected int timeout;
	protected Address address;
	private Channel channel;
	private LockManager lockManager;
	private Map<LockDecree, Lock> lockMap = new HashMap<LockDecree, Lock>();
	
	/**
	 * Constructs a new DistributableLock.
	 * @param <D> either java.sql.Driver or javax.sql.Datasource
	 * @param databaseCluster a database cluster
	 * @param decorator a decorator
	 * @throws Exception
	 */
	public <D> DistributableLockManager(DatabaseCluster<D> databaseCluster, DistributableDatabaseClusterDecorator decorator) throws Exception
	{
		this.lockManager = databaseCluster.getLockManager();
		this.channel = decorator.createChannel(databaseCluster.getId() + "-lock");
		this.address = this.channel.getLocalAddress();
		this.timeout = decorator.getTimeout();

		this.votingAdapter = new VotingAdapter(this.channel);

		this.votingAdapter.addVoteListener(this);
	}

	public void start() throws Exception
	{
		this.channel.connect(this.channel.getClusterName());
		
		this.lockManager.start();
	}
	
	public void stop()
	{
		this.channel.close();

		this.lockManager.stop();
	}
	
	/**
	 * Read locks are local.
	 * @see net.sf.hajdbc.LockManager#readLock(java.lang.String)
	 */
	public Lock readLock(String object)
	{
		return this.lockManager.readLock(object);
	}

	/**
	 * Write locks are distributed.
	 * @see net.sf.hajdbc.LockManager#writeLock(java.lang.String)
	 */
	public Lock writeLock(String object)
	{
		return new DistributableLock(object);
	}

	/**
	 * @see org.jgroups.blocks.VotingListener#vote(java.lang.Object)
	 */
	@Override
	public boolean vote(Object decree) throws VoteException
	{
		if ((decree == null) || !(decree instanceof LockDecree)) throw new VoteException("");
		
		return ((LockDecree) decree).vote(this.lockManager, this.lockMap);
	}
	
	/**
	 * @see org.jgroups.MembershipListener#block()
	 */
	@Override
	public void block()
	{
		// Do nothing
	}

	/**
	 * @see org.jgroups.MembershipListener#suspect(org.jgroups.Address)
	 */
	@Override
	public void suspect(Address address)
	{
		// Do nothing
	}

	/**
	 * @see org.jgroups.MembershipListener#viewAccepted(org.jgroups.View)
	 */
	@Override
	public void viewAccepted(View view)
	{
		synchronized (this.lockMap)
		{
			Iterator<Map.Entry<LockDecree, Lock>> lockMapEntries = this.lockMap.entrySet().iterator();
			
			while (lockMapEntries.hasNext())
			{
				Map.Entry<LockDecree, Lock> lockMapEntry = lockMapEntries.next();
				
				if (!view.containsMember(lockMapEntry.getKey().getAddress()))
				{
					lockMapEntry.getValue().unlock();
					
					lockMapEntries.remove();
				}
			}
		}
	}

	private class DistributableLock implements Lock
	{
		private String object;
		
		public DistributableLock(String object)
		{
			this.object = object;
		}
		
		/**
		 * @see java.util.concurrent.locks.Lock#lock()
		 */
		public void lock()
		{
			while (!this.tryLock());
		}

		/**
		 * @see java.util.concurrent.locks.Lock#lockInterruptibly()
		 */
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
		public boolean tryLock()
		{
			try
			{
				boolean locked = DistributableLockManager.this.votingAdapter.vote(new AcquireLockDecree(this.object, DistributableLockManager.this.address), DistributableLockManager.this.timeout);
				
				if (!locked)
				{
					this.unlock();
				}
				
				return locked;
			}
			catch (ChannelException e)
			{
				throw new IllegalStateException(e);
			}
		}

		/**
		 * @see java.util.concurrent.locks.Lock#tryLock(long, java.util.concurrent.TimeUnit)
		 */
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
		public void unlock()
		{
			try
			{
				DistributableLockManager.this.votingAdapter.vote(new ReleaseLockDecree(this.object, DistributableLockManager.this.address), DistributableLockManager.this.timeout);
			}
			catch (ChannelException e)
			{
				throw new IllegalStateException(e);
			}
		}

		/**
		 * @see java.util.concurrent.locks.Lock#newCondition()
		 */
		public Condition newCondition()
		{
			throw new UnsupportedOperationException();
		}
	}
}