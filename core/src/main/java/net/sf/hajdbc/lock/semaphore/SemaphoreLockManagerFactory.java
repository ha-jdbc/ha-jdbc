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
package net.sf.hajdbc.lock.semaphore;

import net.sf.hajdbc.lock.LockManager;
import net.sf.hajdbc.lock.LockManagerFactory;

public class SemaphoreLockManagerFactory implements LockManagerFactory
{
	private static final long serialVersionUID = -1330668107554832289L;

	private boolean fair;
	
	public void setFair(boolean fair)
	{
		this.fair = fair;
	}
	
	public boolean isFair()
	{
		return this.fair;
	}

	@Override
	public String getId()
	{
		return "semaphore";
	}
	
	@Override
	public LockManager createLockManager()
	{
		return new SemaphoreLockManager(this.fair);
	}
}
