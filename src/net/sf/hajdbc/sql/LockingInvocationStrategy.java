/*
 * HA-JDBC: High-Availability JDBC
 * Copyright (c) 2004-2008 Paul Ferraro
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
package net.sf.hajdbc.sql;

import java.util.List;
import java.util.concurrent.locks.Lock;

/**
 * An invocation strategy decorator that acquires a list of locks before invocation, and releases them afterward.
 * @author Paul Ferraro
 * @param <D> Type of the root object (e.g. driver, datasource)
 * @param <T> Target object type of the invocation
 * @param <R> Return type of this invocation
 */
public class LockingInvocationStrategy<D, T, R> implements InvocationStrategy<D, T, R>
{
	private InvocationStrategy<D, T, R> strategy;
	private List<Lock> lockList;
	
	/**
	 * @param strategy
	 * @param lockList
	 */
	public LockingInvocationStrategy(InvocationStrategy<D, T, R> strategy, List<Lock> lockList)
	{
		this.strategy = strategy;
		this.lockList = lockList;
	}

	/**
	 * @see net.sf.hajdbc.sql.InvocationStrategy#invoke(net.sf.hajdbc.sql.SQLProxy, net.sf.hajdbc.sql.Invoker)
	 */
	@Override
	public R invoke(SQLProxy<D, T> proxy, Invoker<D, T, R> invoker) throws Exception
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
