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
package net.sf.hajdbc.sql;

import java.util.List;
import java.util.concurrent.locks.Lock;

import net.sf.hajdbc.Database;

/**
 * An invocation strategy decorator that acquires a list of locks before invocation, and releases them afterward.
 * @author Paul Ferraro
 * @param <D> Type of the root object (e.g. driver, datasource)
 * @param <T> Target object type of the invocation
 * @param <R> Return type of this invocation
 */
public class LockingInvocationStrategy<Z, D extends Database<Z>, T, R, E extends Exception> implements InvocationStrategy<Z, D, T, R, E>
{
	private InvocationStrategy<Z, D, T, R, E> strategy;
	private List<Lock> lockList;
	
	/**
	 * @param strategy
	 * @param lockList
	 */
	public LockingInvocationStrategy(InvocationStrategy<Z, D, T, R, E> strategy, List<Lock> lockList)
	{
		this.strategy = strategy;
		this.lockList = lockList;
	}

	/**
	 * @see net.sf.hajdbc.sql.InvocationStrategy#invoke(net.sf.hajdbc.sql.SQLProxy, net.sf.hajdbc.sql.Invoker)
	 */
	@Override
	public R invoke(SQLProxy<Z, D, T, E> proxy, Invoker<Z, D, T, R, E> invoker) throws E
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
