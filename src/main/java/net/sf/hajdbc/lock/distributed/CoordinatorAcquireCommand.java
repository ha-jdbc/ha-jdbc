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

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;

/**
 * Acquire lock command for execution on group coordinator.
 * @author Paul Ferraro
 */
public class CoordinatorAcquireCommand extends CoordinatorLockCommand<Boolean>
{
	private static final long serialVersionUID = 1725113200306907771L;
	
	private final long timeout;
	
	public CoordinatorAcquireCommand(RemoteLockDescriptor descriptor, long timeout)
	{
		super(descriptor);

		this.timeout = timeout;
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.lock.distributed.CoordinatorLockCommand#execute(java.util.concurrent.locks.Lock)
	 */
	@Override
	protected Boolean execute(Lock lock)
	{
		try
		{
			return lock.tryLock(this.timeout, TimeUnit.MILLISECONDS);
		}
		catch (InterruptedException e)
		{
			Thread.currentThread().interrupt();
			
			return false;
		}
	}
}
