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

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;

import net.sf.hajdbc.LockManager;
import net.sf.hajdbc.Messages;

import org.jgroups.Channel;
import org.jgroups.ChannelException;
import org.jgroups.blocks.TwoPhaseVotingAdapter;
import org.jgroups.blocks.TwoPhaseVotingListener;
import org.jgroups.blocks.VoteResponseProcessor;
import org.jgroups.blocks.VotingAdapter;
import org.jgroups.blocks.VotingAdapter.FailureVoteResult;
import org.jgroups.blocks.VotingAdapter.VoteResult;
import org.jgroups.util.Rsp;
import org.jgroups.util.RspList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Paul Ferraro
 */
public class DistributableLockManager implements LockManager, TwoPhaseVotingListener, VoteResponseProcessor
{
	private static Logger logger = LoggerFactory.getLogger(DistributableLockManager.class);
	
	private static enum LockDecreeType { RELEASE, ACQUIRE };
	
	private Channel channel;
	protected int timeout;
	private LockManager localLockManager;
	private Map<String, Lock> lockMap = new HashMap<String, Lock>();
	protected TwoPhaseVotingAdapter votingAdapter;
	
	/**
	 * Constructs a new DistributableLock.
	 * @param name
	 * @param protocol 
	 * @param timeout 
	 * @param lock 
	 * @throws Exception
	 */
	public DistributableLockManager(Channel channel, int timeout, LockManager localLockManager) throws Exception
	{
		this.timeout = timeout;
		this.localLockManager = localLockManager;
		this.channel = channel;
		
		this.votingAdapter = new TwoPhaseVotingAdapter(new VotingAdapter(this.channel));
		
		this.votingAdapter.addListener(this);
	}

	public void start() throws ChannelException
	{
		this.channel.connect(this.channel.getChannelName());
	}
	
	public void stop()
	{
		this.channel.close();
	}
	
	/**
	 * @see net.sf.hajdbc.LockManager#readLock(java.lang.String)
	 */
	public Lock readLock(String object)
	{
		return this.localLockManager.readLock(object);
	}

	/**
	 * @see net.sf.hajdbc.LockManager#writeLock(java.lang.String)
	 */
	public Lock writeLock(String object)
	{
		return this.getDistributableLock(object);
	}

	private synchronized Lock getDistributableLock(String object)
	{
		Lock lock = this.lockMap.get(object);
		
		if (lock == null)
		{
			lock = new DistributableLock(object);
			
			this.lockMap.put(object, lock);
		}
		
		return lock;
	}

	/**
	 * @see org.jgroups.blocks.VoteResponseProcessor#processResponses(org.jgroups.util.RspList, int, java.lang.Object)
	 */
	public boolean processResponses(RspList responseMap, int consensusType, Object decree) throws ChannelException
	{
		if (responseMap == null) return false;

		Iterator responses = responseMap.values().iterator();
		
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
	
	/**
	 * @see org.jgroups.blocks.TwoPhaseVotingListener#prepare(java.lang.Object)
	 */
	public boolean prepare(Object object)
	{
		LockDecree decree = LockDecree.class.cast(object);
		
		if (decree.getType() == LockDecreeType.ACQUIRE)
		{
			return this.getLocalLock(decree).tryLock();
		}
		
		return true;
	}

	/**
	 * @see org.jgroups.blocks.TwoPhaseVotingListener#commit(java.lang.Object)
	 */
	public boolean commit(Object object)
	{
		LockDecree decree = LockDecree.class.cast(object);

		if (decree.getType() == LockDecreeType.RELEASE)
		{
			this.getLocalLock(decree).unlock();
		}
		
		return true;
	}

	/**
	 * @see org.jgroups.blocks.TwoPhaseVotingListener#abort(java.lang.Object)
	 */
	public void abort(Object object)
	{
		LockDecree decree = LockDecree.class.cast(object);
		
		if (decree.getType() == LockDecreeType.ACQUIRE)
		{
			this.getLocalLock(decree).unlock();
		}
	}
	
	private Lock getLocalLock(LockDecree decree)
	{
		return this.localLockManager.writeLock(decree.getId());
	}
	
	/**
	 * @return the channel used by the lock manager
	 */
	public Channel getChannel()
	{
		return this.channel;
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
				return DistributableLockManager.this.votingAdapter.vote(new LockDecree(this.object, LockDecreeType.ACQUIRE), DistributableLockManager.this.timeout, DistributableLockManager.this);
			}
			catch (ClassCastException e)
			{
				throw new IllegalStateException(e);
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
				if (!DistributableLockManager.this.votingAdapter.vote(new LockDecree(this.object, LockDecreeType.RELEASE), DistributableLockManager.this.timeout, DistributableLockManager.this))
				{
					throw new IllegalStateException();
				}
			}
			catch (ClassCastException e)
			{
				throw new IllegalStateException(e);
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
	
	public static class LockDecree implements Externalizable
	{
		private static final long serialVersionUID = -3362590132133718171L;
		
		private String object;
		private LockDecreeType type;
		
		public LockDecree(String id, LockDecreeType type)
		{
			this.object = id;
			this.type = type;
		}

		public LockDecree()
		{
			// Required for deserialization
		}
		
		public String getId()
		{
			return this.object;
		}

		public LockDecreeType getType()
		{
			return this.type;
		}

		/**
		 * @see java.io.Externalizable#writeExternal(java.io.ObjectOutput)
		 */
		public void writeExternal(ObjectOutput output) throws IOException
		{
			output.writeUTF(this.object);
			output.writeInt(this.type.ordinal());
		}

		/**
		 * @see java.io.Externalizable#readExternal(java.io.ObjectInput)
		 */
		public void readExternal(ObjectInput input) throws IOException
		{
			this.object = input.readUTF();
			this.type = LockDecreeType.values()[input.readInt()];
		}
	}
}