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
package net.sf.hajdbc.distributable;

import java.util.Iterator;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;

import net.sf.hajdbc.DatabaseCluster;
import net.sf.hajdbc.LockManager;
import net.sf.hajdbc.Messages;

import org.jgroups.Channel;
import org.jgroups.ChannelException;
import org.jgroups.blocks.TwoPhaseVotingAdapter;
import org.jgroups.blocks.TwoPhaseVotingListener;
import org.jgroups.blocks.VoteException;
import org.jgroups.blocks.VoteResponseProcessor;
import org.jgroups.blocks.VotingAdapter;
import org.jgroups.blocks.VotingAdapter.FailureVoteResult;
import org.jgroups.blocks.VotingAdapter.VoteResult;
import org.jgroups.util.Rsp;
import org.jgroups.util.RspList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * LockManager implementation that leverages a JGroups 2-phase voting adapter for obtain remote write locks.
 * 
 * @author Paul Ferraro
 */
public class DistributableLockManager implements LockManager, TwoPhaseVotingListener
{
	static Logger logger = LoggerFactory.getLogger(DistributableLockManager.class);
	
	protected TwoPhaseVotingAdapter votingAdapter;
	protected int timeout;
	private Channel channel;
	private LockManager lockManager;
	
	/**
	 * Constructs a new DistributableLock.
	 * @param channel
	 * @param timeout 
	 * @param lockManager 
	 * @throws Exception
	 */
	public <D> DistributableLockManager(DatabaseCluster<D> databaseCluster, DistributableDatabaseClusterDecorator decorator) throws Exception
	{
		this.lockManager = databaseCluster.getLockManager();
		this.channel = decorator.createChannel(databaseCluster.getId() + "lock");
		this.timeout = decorator.getTimeout();
		
		this.votingAdapter = new TwoPhaseVotingAdapter(new VotingAdapter(this.channel));
		
		this.votingAdapter.addListener(this);
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
	 * @see org.jgroups.blocks.TwoPhaseVotingListener#prepare(java.lang.Object)
	 */
	public boolean prepare(Object object) throws VoteException
	{
		return this.toLockDecree(object).prepare(this.lockManager);
	}

	/**
	 * @see org.jgroups.blocks.TwoPhaseVotingListener#commit(java.lang.Object)
	 */
	public boolean commit(Object object) throws VoteException
	{
		return this.toLockDecree(object).commit(this.lockManager);
	}

	/**
	 * @see org.jgroups.blocks.TwoPhaseVotingListener#abort(java.lang.Object)
	 */
	public void abort(Object object) throws VoteException
	{
		this.toLockDecree(object).abort(this.lockManager);
	}
	
	private LockDecree toLockDecree(Object object) throws VoteException
	{
		if (LockDecree.class.isInstance(object)) throw new VoteException("");
		
		return LockDecree.class.cast(object);
	}
	
	private class DistributableLock implements Lock, VoteResponseProcessor
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
				return DistributableLockManager.this.votingAdapter.vote(new AcquireLockDecree(this.object), DistributableLockManager.this.timeout, this);
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
				DistributableLockManager.this.votingAdapter.vote(new ReleaseLockDecree(this.object), DistributableLockManager.this.timeout, this);
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
		
		/**
		 * @see org.jgroups.blocks.VoteResponseProcessor#processResponses(org.jgroups.util.RspList, int, java.lang.Object)
		 */
		public boolean processResponses(RspList responseMap, int consensusType, Object decree) throws ChannelException
		{
			if (responseMap == null) return false;

			Iterator<?> responses = responseMap.values().iterator();
			
			while (responses.hasNext())
			{
				Rsp response = Rsp.class.cast(responses.next());
				
				if (response.wasSuspected()) continue;
				
				if (!response.wasReceived())
				{
					throw new ChannelException(Messages.getMessage(Messages.VOTE_NO_RESPONSE, response.getSender()));
				}
				
				Object value = response.getValue();
				
				if (value == null) continue;

				if (Throwable.class.isInstance(value))
				{
					throw new ChannelException(Messages.getMessage(Messages.VOTE_ERROR_RESPONSE, response.getSender()), Throwable.class.cast(value));
				}
				
				if (!VoteResult.class.isInstance(value))
				{
					throw new ChannelException(Messages.getMessage(Messages.VOTE_INVALID_RESPONSE, response.getSender(), value.getClass().getName()));
				}
				
				VoteResult result = VoteResult.class.cast(value);
				
				if (FailureVoteResult.class.isInstance(result))
				{
					logger.error(FailureVoteResult.class.cast(value).getReason());
					
					return false;
				}
				
				if (result.getNegativeVotes() > 0) return false;
			}
			
			return true;
		}		
	}
}