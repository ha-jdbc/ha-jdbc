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

import java.util.Map;
import java.util.concurrent.locks.Lock;

import net.sf.hajdbc.distributed.Command;

/**
 * @author Paul Ferraro
 *
 */
public class MemberAcquireLockCommand implements Command<Boolean, LockCommandContext>
{
	private static final long serialVersionUID = 673191217118566395L;

	private final RemoteLockDescriptor descriptor;
	
	public MemberAcquireLockCommand(RemoteLockDescriptor descriptor)
	{
		this.descriptor = descriptor;
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.distributed.Command#execute(java.lang.Object)
	 */
	@Override
	public Boolean execute(LockCommandContext context)
	{
		Lock lock = context.getLock(this.descriptor);
		
		boolean locked = lock.tryLock();
		
		if (locked)
		{
			Map<LockDescriptor, Lock> lockMap = context.getRemoteLocks(this.descriptor);
			
			synchronized (lockMap)
			{
				lockMap.put(this.descriptor, lock);
			}
		}
		
		return locked;
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.distributed.Command#marshalResult(java.lang.Object)
	 */
	@Override
	public Object marshalResult(Boolean result)
	{
		return result;
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.distributed.Command#unmarshalResult(java.lang.Object)
	 */
	@Override
	public Boolean unmarshalResult(Object object)
	{
		return (Boolean) object;
	}

	/**
	 * {@inheritDoc}
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString()
	{
		return String.format("%s(%s)", this.getClass().getSimpleName(), this.descriptor);
	}
}
