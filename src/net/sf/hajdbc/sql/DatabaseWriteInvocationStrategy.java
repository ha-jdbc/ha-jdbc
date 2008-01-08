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
package net.sf.hajdbc.sql;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.locks.Lock;

import net.sf.hajdbc.Database;
import net.sf.hajdbc.DatabaseCluster;
import net.sf.hajdbc.Messages;
import net.sf.hajdbc.util.SQLExceptionFactory;

/**
 * @author Paul Ferraro
 *
 */
public abstract class DatabaseWriteInvocationStrategy<D, T, R> implements InvocationStrategy<D, T, R>
{
	/**
	 * @see net.sf.hajdbc.sql.InvocationStrategy#invoke(net.sf.hajdbc.sql.SQLProxy, net.sf.hajdbc.sql.Invoker)
	 */
	@Override
	public R invoke(SQLProxy<D, T> proxy, Invoker<D, T, R> invoker) throws Exception
	{
		SortedMap<Database<D>, R> map = this.invokeAll(proxy, invoker);
		
		return map.get(map.firstKey());
	}
	
	protected SortedMap<Database<D>, R> invokeAll(SQLProxy<D, T> proxy, final Invoker<D, T, R> invoker) throws Exception
	{
		SortedMap<Database<D>, R> resultMap = new TreeMap<Database<D>, R>();
		SortedMap<Database<D>, SQLException> exceptionMap = new TreeMap<Database<D>, SQLException>();
		Map<Database<D>, Future<R>> futureMap = new HashMap<Database<D>, Future<R>>();

		DatabaseCluster<D> cluster = proxy.getDatabaseCluster();
		
		List<Lock> lockList = this.getLockList(cluster);
		
		for (Lock lock: lockList)
		{
			lock.lock();
		}
		
		try
		{
			Set<Database<D>> databaseSet = cluster.getBalancer().all();
			
			proxy.getRoot().retain(databaseSet);
			
			if (databaseSet.isEmpty())
			{
				throw new SQLException(Messages.getMessage(Messages.NO_ACTIVE_DATABASES, cluster));
			}
			
			ExecutorService executor = this.getExecutor(cluster);
			
			for (final Database<D> database: databaseSet)
			{
				final T object = proxy.getObject(database);
				
				Callable<R> task = new Callable<R>()
				{
					public R call() throws Exception
					{
						return invoker.invoke(database, object);
					}
				};
	
				futureMap.put(database, executor.submit(task));
			}

			for (Map.Entry<Database<D>, Future<R>> futureMapEntry: futureMap.entrySet())
			{
				Database<D> database = futureMapEntry.getKey();
				
				try
				{
					resultMap.put(database, futureMapEntry.getValue().get());
				}
				catch (ExecutionException e)
				{
					exceptionMap.put(database, SQLExceptionFactory.createSQLException(e.getCause()));
				}
				catch (InterruptedException e)
				{
					Thread.currentThread().interrupt();
					
					exceptionMap.put(database, SQLExceptionFactory.createSQLException(e));
				}
			}
		}
		finally
		{
			for (Lock lock: lockList)
			{
				lock.unlock();
			}
		}
		
		// If no databases returned successfully, return an exception back to the caller
		if (resultMap.isEmpty())
		{
			proxy.handleFailures(exceptionMap);
		}
		
		// If any databases failed, while others succeeded, handle the failures
		return exceptionMap.isEmpty() ? resultMap : proxy.handlePartialFailure(resultMap, exceptionMap);
	}
	
	protected abstract ExecutorService getExecutor(DatabaseCluster<D> cluster);
	
	protected abstract List<Lock> getLockList(DatabaseCluster<D> cluster);
}