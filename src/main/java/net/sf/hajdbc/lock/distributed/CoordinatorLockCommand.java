/*
 * HA-JDBC: High-Availability JDBC
 * Copyright (C) 2012  Paul Ferraro
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

import java.util.concurrent.locks.Lock;

import net.sf.hajdbc.distributed.Command;

/**
 * A lock command to be executed on the group coordinator.
 * @author Paul Ferraro
 */
public abstract class CoordinatorLockCommand<R> implements Command<R, LockCommandContext>
{
	private static final long serialVersionUID = 5921849426289256348L;
	
	private final RemoteLockDescriptor descriptor;

	protected CoordinatorLockCommand(RemoteLockDescriptor descriptor)
	{
		this.descriptor = descriptor;
	}
	
	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.distributed.Command#execute(java.lang.Object)
	 */
	@Override
	public R execute(LockCommandContext context)
	{
		return this.execute(context.getLock(this.descriptor));
	}
	
	protected abstract R execute(Lock lock);

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
