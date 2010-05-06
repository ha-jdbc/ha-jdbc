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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.AbstractMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import net.sf.hajdbc.Database;
import net.sf.hajdbc.DatabaseCluster;
import net.sf.hajdbc.ExceptionFactory;
import net.sf.hajdbc.Messages;

/**
 * @author Paul Ferraro
 * @param <D> 
 * @param <T> 
 * @param <R> 
 */
public class InvokeOnAllInvocationStrategy extends InvokeOnManyInvocationStrategy
{
	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.sql.InvokeOnManyInvocationStrategy#collectResults(net.sf.hajdbc.sql.SQLProxy, net.sf.hajdbc.sql.Invoker)
	 */
	@Override
	protected <Z, D extends Database<Z>, T, R, E extends Exception> Map.Entry<SortedMap<D, R>, SortedMap<D, E>> collectResults(SQLProxy<Z, D, T, E> proxy, final Invoker<Z, D, T, R, E> invoker)
	{
		DatabaseCluster<Z, D> cluster = proxy.getDatabaseCluster();
		Set<D> databaseSet = cluster.getBalancer();
		
		proxy.getRoot().retain(databaseSet);
		
		ExceptionFactory<E> exceptionFactory = proxy.getExceptionFactory();
		
		if (databaseSet.isEmpty())
		{
			exceptionFactory.createException(Messages.NO_ACTIVE_DATABASES.getMessage(cluster));
		}

		int size = databaseSet.size();
		
		List<Invocation<Z, D, T, R, E>> invocationList = new ArrayList<Invocation<Z, D, T, R, E>>(size);
		
		for (D database: databaseSet)
		{
			invocationList.add(new Invocation<Z, D, T, R, E>(invoker, database, proxy.getObject(database)));
		}
		
		try
		{
			List<Future<R>> futureList = this.getExecutor(cluster).invokeAll(invocationList);
			
			final SortedMap<D, R> resultMap = new TreeMap<D, R>();
			final SortedMap<D, E> exceptionMap = new TreeMap<D, E>();
			
			for (int i = 0; i < invocationList.size(); ++i)
			{
				D database = invocationList.get(i).getDatabase();
				
				try
				{
					resultMap.put(database, futureList.get(i).get());
				}
				catch (ExecutionException e)
				{
					exceptionMap.put(database, exceptionFactory.createException(e.getCause()));
				}
				catch (InterruptedException e)
				{
					Thread.currentThread().interrupt();
					
					exceptionMap.put(database, exceptionFactory.createException(e));
				}
			}
		
			return new AbstractMap.SimpleImmutableEntry<SortedMap<D, R>, SortedMap<D, E>>(resultMap, exceptionMap);
		}
		catch (InterruptedException e)
		{
			throw new IllegalStateException(e);
		}
	}
	
	protected <Z, D extends Database<Z>> ExecutorService getExecutor(DatabaseCluster<Z, D> cluster)
	{
		return cluster.getExecutor();
	}
	
	private class Invocation<Z, D extends Database<Z>, T, R, E extends Exception> implements Callable<R>
	{
		private final Invoker<Z, D, T, R, E> invoker;
		private final D database;
		private final T object;
		
		Invocation(Invoker<Z, D, T, R, E> invoker, D database, T object)
		{
			this.invoker = invoker;
			this.database = database;
			this.object = object;
		}
		
		D getDatabase()
		{
			return this.database;
		}
		
		/**
		 * {@inheritDoc}
		 * @see java.util.concurrent.Callable#call()
		 */
		@Override
		public R call() throws E
		{
			return this.invoker.invoke(this.database, this.object);
		}
	}
}
