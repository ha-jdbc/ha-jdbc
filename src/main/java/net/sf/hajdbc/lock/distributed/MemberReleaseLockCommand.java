/*
 * HA-JDBC: High-Availability JDBC
 * Copyright (c) 2004-2009 Paul Ferraro
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
package net.sf.hajdbc.lock.distributed;

import java.util.Map;
import java.util.concurrent.locks.Lock;

import net.sf.hajdbc.distributed.Command;

/**
 * @author paul
 *
 */
public class MemberReleaseLockCommand implements Command<Void, LockCommandContext>
{
	private static final long serialVersionUID = -4088487420468046409L;

	private final RemoteLockDescriptor descriptor;
	
	public MemberReleaseLockCommand(RemoteLockDescriptor descriptor)
	{
		this.descriptor = descriptor;
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.distributed.Command#execute(java.lang.Object)
	 */
	@Override
	public Void execute(LockCommandContext context)
	{
		Map<LockDescriptor, Lock> locks = context.getRemoteLocks(this.descriptor);
		
		if (locks != null)
		{
			Lock lock = null;
			
			synchronized (locks)
			{
				lock = locks.remove(this.descriptor);
			}
			
			if (lock != null)
			{
				lock.unlock();
			}
		}

		return null;
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.distributed.Command#marshalResult(java.lang.Object)
	 */
	@Override
	public Object marshalResult(Void result)
	{
		return result;
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.distributed.Command#unmarshalResult(java.lang.Object)
	 */
	@Override
	public Void unmarshalResult(Object object)
	{
		return (Void) object;
	}
}
