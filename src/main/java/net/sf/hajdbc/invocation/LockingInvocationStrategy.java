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
package net.sf.hajdbc.invocation;

import java.util.List;
import java.util.SortedMap;
import java.util.concurrent.locks.Lock;

import net.sf.hajdbc.Database;
import net.sf.hajdbc.sql.ProxyFactory;

/**
 * An invocation strategy decorator that acquires a list of locks before invocation, and releases them afterward.
 * @author Paul Ferraro
 */
public class LockingInvocationStrategy implements InvocationStrategy
{
	private InvocationStrategy strategy;
	private List<Lock> lockList;
	
	/**
	 * @param strategy
	 * @param lockList
	 */
	public LockingInvocationStrategy(InvocationStrategy strategy, List<Lock> lockList)
	{
		this.strategy = strategy;
		this.lockList = lockList;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public <Z, D extends Database<Z>, T, R, E extends Exception> SortedMap<D, R> invoke(ProxyFactory<Z, D, T, E> proxy, Invoker<Z, D, T, R, E> invoker) throws E
	{
		for (Lock lock: this.lockList)
		{
			lock.lock();
		}
		
		try
		{
			return this.strategy.invoke(proxy, invoker);
		}
		finally
		{
			for (Lock lock: this.lockList)
			{
				lock.unlock();
			}
		}
	}
}
