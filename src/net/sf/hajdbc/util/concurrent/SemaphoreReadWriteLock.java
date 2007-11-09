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
package net.sf.hajdbc.util.concurrent;

import java.util.concurrent.Semaphore;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;

/**
 * Simple {@link java.util.concurrent.lock.ReadWriteLock} implementation that uses a semaphore that grants up to {@link java.lang.Integer#MAX_VALUE} permits using a fair FIFO policy.
 * A read lock requires 1 permit, while a write lock requires all the permits.
 * 
 * @author Paul Ferraro
 */
public class SemaphoreReadWriteLock implements ReadWriteLock
{
	private Semaphore semaphore = new Semaphore(Integer.MAX_VALUE, true);
	private Lock readLock = new SemaphoreLock(this.semaphore, 1);
	private Lock writeLock = new SemaphoreLock(this.semaphore, Integer.MAX_VALUE);
	
	/**
	 * @see java.util.concurrent.locks.ReadWriteLock#readLock()
	 */
	@Override
	public Lock readLock()
	{
		return this.readLock;
	}

	/**
	 * @see java.util.concurrent.locks.ReadWriteLock#writeLock()
	 */
	@Override
	public Lock writeLock()
	{
		return this.writeLock;
	}
}
