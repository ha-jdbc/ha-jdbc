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

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;

import org.jgroups.Channel;
import org.jgroups.ChannelException;
import org.jgroups.JChannel;
import org.jgroups.blocks.DistributedLockManager;
import org.jgroups.blocks.LockManager;
import org.jgroups.blocks.LockNotGrantedException;
import org.jgroups.blocks.LockNotReleasedException;
import org.jgroups.blocks.TwoPhaseVotingAdapter;
import org.jgroups.blocks.TwoPhaseVotingListener;
import org.jgroups.blocks.VoteException;
import org.jgroups.blocks.VotingAdapter;

/**
 * @author  Paul Ferraro
 * @since   1.1
 */
public class DistributableLock implements Lock, TwoPhaseVotingListener
{
	private LockManager lockManager;
	private Channel channel;
	private int timeout;
	private Lock lock;
	
	/**
	 * Constructs a new DistributableLock.
	 * @param name
	 * @param protocol 
	 * @param timeout 
	 * @param lock 
	 * @throws Exception
	 */
	public DistributableLock(String name, String protocol, int timeout, Lock lock) throws Exception
	{
		this.timeout = timeout;
		this.lock = lock;
		this.channel = new JChannel(protocol);
		this.channel.connect(name);
		
		TwoPhaseVotingAdapter adapter = new TwoPhaseVotingAdapter(new VotingAdapter(this.channel));
		
		adapter.addListener(this);
		
		this.lockManager = new DistributedLockManager(adapter, name);
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
			this.lockManager.lock(this.getObject(), this.getOwner(), this.timeout);
			
			return true;
		}
		catch (LockNotGrantedException e)
		{
			return false;
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
			this.lockManager.unlock(this.getObject(), this.getOwner());
		}
		catch (ClassCastException e)
		{
			throw new IllegalStateException(e);
		}
		catch (ChannelException e)
		{
			throw new IllegalStateException(e);
		}
		catch (LockNotReleasedException e)
		{
			throw new IllegalStateException(e);
		}		
	}

	/**
	 * @see java.util.concurrent.locks.Lock#newCondition()
	 */
	public Condition newCondition()
	{
		return this.lock.newCondition();
	}
	
	/**
	 * @see org.jgroups.blocks.TwoPhaseVotingListener#prepare(java.lang.Object)
	 */
	public boolean prepare(Object decree) throws VoteException
	{
		if (DistributedLockManager.AcquireLockDecree.class.isInstance(decree))
		{
			return this.lock.tryLock();
		}
		
		return true;
	}

	/**
	 * @see org.jgroups.blocks.TwoPhaseVotingListener#commit(java.lang.Object)
	 */
	public boolean commit(Object decree) throws VoteException
	{
		if (DistributedLockManager.ReleaseLockDecree.class.isInstance(decree))
		{
			this.lock.unlock();
		}
		
		return true;
	}

	/**
	 * @see org.jgroups.blocks.TwoPhaseVotingListener#abort(java.lang.Object)
	 */
	public void abort(Object decree) throws VoteException
	{
		if (DistributedLockManager.AcquireLockDecree.class.isInstance(decree))
		{
			this.lock.unlock();
		}
	}
	
	/**
	 * @return the channel used by the lock manager
	 */
	public Channel getChannel()
	{
		return this.channel;
	}

	private Object getObject()
	{
		return "";
	}
	
	private Object getOwner()
	{
		return Thread.currentThread().getId();
	}
}
