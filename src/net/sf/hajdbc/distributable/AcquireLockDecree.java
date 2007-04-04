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

import net.sf.hajdbc.LockManager;

/**
 * @author Paul Ferraro
 * @since 2.0
 */
public class AcquireLockDecree extends LockDecree
{
	private static final long serialVersionUID = 9016576896686385467L;

	public AcquireLockDecree()
	{
		super();
	}

	public AcquireLockDecree(String id)
	{
		super(id);
	}

	/**
	 * @see net.sf.hajdbc.distributable.LockDecree#prepare(net.sf.hajdbc.LockManager)
	 */
	@Override
	public boolean prepare(LockManager lockManager)
	{
		return this.getLock(lockManager).tryLock();
	}

	/**
	 * @see net.sf.hajdbc.distributable.LockDecree#commit(net.sf.hajdbc.LockManager)
	 */
	@Override
	public boolean commit(LockManager lockManager)
	{
		return true;
	}

	/**
	 * @see net.sf.hajdbc.distributable.LockDecree#abort(net.sf.hajdbc.LockManager)
	 */
	@Override
	public void abort(LockManager lockManager)
	{
		this.getLock(lockManager).unlock();
	}
}