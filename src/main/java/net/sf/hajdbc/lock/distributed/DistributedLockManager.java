/*
 * HA-JDBC: High-Availability JDBC
 * Copyright 2004-2009 Paul Ferraro
 * 
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 * 
 * You should have received a copy of the GNU Lesser General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package net.sf.hajdbc.lock.distributed;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;

import net.sf.hajdbc.Database;
import net.sf.hajdbc.DatabaseCluster;
import net.sf.hajdbc.distributed.CommandDispatcher;
import net.sf.hajdbc.distributed.CommandDispatcherFactory;
import net.sf.hajdbc.distributed.Member;
import net.sf.hajdbc.distributed.MembershipListener;
import net.sf.hajdbc.distributed.Remote;
import net.sf.hajdbc.distributed.Stateful;
import net.sf.hajdbc.lock.LockManager;

/**
 * @author Paul Ferraro
 */
public class DistributedLockManager implements LockManager, LockCommandContext, Stateful, MembershipListener
{
	final CommandDispatcher<LockCommandContext> dispatcher;
	
	private final LockManager lockManager;
	private final ConcurrentMap<Member, Map<LockDescriptor, Lock>> remoteLockDescriptorMap = new ConcurrentHashMap<Member, Map<LockDescriptor, Lock>>();
	
	public <Z, D extends Database<Z>> DistributedLockManager(DatabaseCluster<Z, D> cluster, CommandDispatcherFactory dispatcherFactory) throws Exception
	{
		this.lockManager = cluster.getLockManager();
		LockCommandContext context = this;
		this.dispatcher = dispatcherFactory.createCommandDispatcher(cluster.getId(), context, this, this);
	}
	
	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.lock.LockManager#readLock(java.lang.String)
	 */
	@Override
	public Lock readLock(String id)
	{
		return this.lockManager.readLock(id);
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.lock.LockManager#writeLock(java.lang.String)
	 */
	@Override
	public Lock writeLock(String id)
	{
		return this.getDistibutedLock(new RemoteLockDescriptorImpl(id, LockType.WRITE, this.dispatcher.getLocal()));
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.lock.distributed.LockCommandContext#getLocalLock(java.lang.String, net.sf.hajdbc.lock.distributed.LockType)
	 */
	@Override
	public Lock getLock(LockDescriptor lock)
	{
		String id = lock.getId();
		
		switch (lock.getType())
		{
			case READ:
			{
				return this.lockManager.readLock(id);
			}
			case WRITE:
			{
				return this.lockManager.writeLock(id);
			}
			default:
			{
				throw new IllegalStateException();
			}
		}
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.lock.distributed.LockCommandContext#getDistibutedLock(java.lang.String, net.sf.hajdbc.lock.distributed.LockType, org.jgroups.Address)
	 */
	@Override
	public Lock getDistibutedLock(RemoteLockDescriptor descriptor)
	{
		return new DistributedLock(descriptor, this.getLock(descriptor), this.dispatcher);
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.Lifecycle#start()
	 */
	@Override
	public void start() throws Exception
	{
		this.lockManager.start();
		this.dispatcher.start();
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.Lifecycle#stop()
	 */
	@Override
	public void stop()
	{
		this.dispatcher.stop();
		this.lockManager.stop();
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.lock.distributed.LockCommandContext#getRemoteLocks(org.jgroups.Address)
	 */
	@Override
	public Map<LockDescriptor, Lock> getRemoteLocks(Remote remote)
	{
		return this.remoteLockDescriptorMap.get(remote.getMember());
	}

	/**
	 * {@inheritDoc}
	 * @see org.jgroups.MessageListener#getState()
	 */
	@Override
	public byte[] getState()
	{
		ByteArrayOutputStream bytes = new ByteArrayOutputStream();
		
		try
		{
			ObjectOutput output = new ObjectOutputStream(bytes);
	
			try
			{
				output.writeInt(this.remoteLockDescriptorMap.size());
				
				for (Map.Entry<Member, Map<LockDescriptor, Lock>> entry: this.remoteLockDescriptorMap.entrySet())
				{
					output.writeObject(entry.getKey());
					
					Set<LockDescriptor> descriptors = entry.getValue().keySet();
					
					output.writeInt(descriptors.size());
					
					for (LockDescriptor descriptor: descriptors)
					{
						output.writeUTF(descriptor.getId());
						output.writeInt(descriptor.getType().ordinal());
					}
				}
				
				return bytes.toByteArray();
			}
			finally
			{
				output.close();
			}
		}
		catch (IOException e)
		{
			throw new IllegalStateException(e);
		}
	}

	/**
	 * {@inheritDoc}
	 * @see org.jgroups.MessageListener#setState(byte[])
	 */
	@Override
	public void setState(byte[] state)
	{
		// Is this valid?  or should we unlock/clear?
		assert this.remoteLockDescriptorMap.isEmpty();
		
		try
		{
			ObjectInput input = new ObjectInputStream(new ByteArrayInputStream(state));
			
			try
			{
				int size = input.readInt();
				
				LockType[] types = LockType.values();
				
				for (int i = 0; i < size; ++i)
				{
					Member member = (Member) input.readObject();
					
					Map<LockDescriptor, Lock> map = new HashMap<LockDescriptor, Lock>();
					
					int locks = input.readInt();
					
					for (int j = 0; j < locks; ++j)
					{
						String id = input.readUTF();
						LockType type = types[input.readInt()];
						
						LockDescriptor descriptor = new RemoteLockDescriptorImpl(id, type, member);
						
						Lock lock = this.getLock(descriptor);
						
						lock.lock();
						
						map.put(descriptor, lock);
					}
					
					this.remoteLockDescriptorMap.put(member, map);
				}
			}
			finally
			{
				input.close();
			}
		}
		catch (ClassNotFoundException e)
		{
			throw new IllegalStateException(e);
		}
		catch (IOException e)
		{
			throw new IllegalStateException(e);
		}
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.distributed.MembershipListener#added(org.jgroups.Address)
	 */
	@Override
	public void added(Member member)
	{
		this.remoteLockDescriptorMap.putIfAbsent(member, new HashMap<LockDescriptor, Lock>());
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.distributed.MembershipListener#removed(org.jgroups.Address)
	 */
	@Override
	public void removed(Member member)
	{
		Map<LockDescriptor, Lock> locks = this.remoteLockDescriptorMap.remove(member);
		
		if (locks != null)
		{
			for (Lock lock: locks.values())
			{
				lock.unlock();
			}
		}
	}
	
	private static class DistributedLock implements Lock
	{
		private final RemoteLockDescriptor descriptor;
		private final Lock lock;
		private final CommandDispatcher<LockCommandContext> dispatcher;
		
		DistributedLock(RemoteLockDescriptor descriptor, Lock lock, CommandDispatcher<LockCommandContext> dispatcher)
		{
			this.descriptor = descriptor;
			this.lock = lock;
			this.dispatcher = dispatcher;
		}
		
		@Override
		public void lock()
		{
			boolean locked = false;
			
			while (!locked)
			{
				if (this.dispatcher.isCoordinator())
				{
					this.lock.lock();
					
					try
					{
						locked = this.lockMembers();
					}
					finally
					{
						if (!locked)
						{
							this.lock.unlock();
						}
					}
				}
				else
				{
					locked = this.lockCoordinator(Long.MAX_VALUE);
				}
				
				if (!locked)
				{
					Thread.yield();
				}
			}
		}

		@Override
		public void lockInterruptibly() throws InterruptedException
		{
			boolean locked = false;
			
			while (!locked)
			{
				if (this.dispatcher.isCoordinator())
				{
					this.lock.lockInterruptibly();
					
					try
					{
						locked = this.lockMembers();
					}
					finally
					{
						if (!locked)
						{
							this.lock.unlock();
						}
					}
				}
				else
				{
					locked = this.lockCoordinator(Long.MAX_VALUE);
				}
				
				if (Thread.currentThread().isInterrupted())
				{
					throw new InterruptedException();
				}
				
				if (!locked)
				{
					Thread.yield();
				}
			}
		}

		@Override
		public boolean tryLock()
		{
			boolean locked = false;
			
			if (this.dispatcher.isCoordinator())
			{
				if (this.lock.tryLock())
				{
					try
					{
						locked = this.lockMembers();
					}
					finally
					{
						if (!locked)
						{
							this.lock.unlock();
						}
					}
				}
			}
			else
			{
				locked = this.lockCoordinator(0);
			}
			
			return locked;
		}

		@Override
		public boolean tryLock(long time, TimeUnit unit) throws InterruptedException
		{
			boolean locked = false;
			
			if (this.dispatcher.isCoordinator())
			{
				if (this.lock.tryLock(time, unit))
				{
					try
					{
						locked = this.lockMembers();
					}
					finally
					{
						if (!locked)
						{
							this.lock.unlock();
						}
					}
				}
			}
			else
			{
				locked = this.lockCoordinator(unit.toMillis(time));
			}
			
			return locked;
		}

		private boolean lockMembers()
		{
			boolean locked = true;
			
			Map<Member, Boolean> results = this.dispatcher.executeAll(new MemberAcquireLockCommand(this.descriptor));
			
			for (Map.Entry<Member, Boolean> entry: results.entrySet())
			{
				locked &= entry.getValue();
			}
			
			if (!locked)
			{
				this.unlockMembers();
			}
			
			return locked;
		}
		
		private boolean lockCoordinator(long timeout)
		{
			return this.dispatcher.executeCoordinator(new CoordinatorAcquireCommand(this.descriptor, timeout));
		}
		
		@Override
		public void unlock()
		{
			if (this.dispatcher.isCoordinator())
			{
				this.unlockMembers();
				
				this.lock.unlock();
			}
			else
			{
				this.unlockCoordinator();
			}
		}
		
		private void unlockMembers()
		{
			this.dispatcher.executeAll(new MemberReleaseLockCommand(this.descriptor));
		}
		
		private void unlockCoordinator()
		{
			this.dispatcher.executeCoordinator(new CoordinatorReleaseCommand(this.descriptor));
		}

		@Override
		public Condition newCondition()
		{
			throw new UnsupportedOperationException();
		}
	}
	
	private static class RemoteLockDescriptorImpl implements RemoteLockDescriptor
	{
		private static final long serialVersionUID = 1950781245453120790L;
		
		private final String id;
		private final LockType type;		
		private final Member member;
		
		RemoteLockDescriptorImpl(String id, LockType type, Member member)
		{
			this.id = id;
			this.type = type;
			this.member = member;
		}
		
		@Override
		public String getId()
		{
			return this.id;
		}

		@Override
		public LockType getType()
		{
			return this.type;
		}

		@Override
		public Member getMember()
		{
			return this.member;
		}

		/**
		 * {@inheritDoc}
		 * @see java.lang.Object#equals(java.lang.Object)
		 */
		@Override
		public boolean equals(Object object)
		{
			if ((object == null) || !(object instanceof RemoteLockDescriptor)) return false;
			
			RemoteLockDescriptor lock = (RemoteLockDescriptor) object;
			
			return this.id.equals(lock.getId());
		}

		/**
		 * {@inheritDoc}
		 * @see java.lang.Object#hashCode()
		 */
		@Override
		public int hashCode()
		{
			return this.id.hashCode();
		}

		/**
		 * {@inheritDoc}
		 * @see java.lang.Object#toString()
		 */
		@Override
		public String toString()
		{
			return this.id;
		}
	}
}
