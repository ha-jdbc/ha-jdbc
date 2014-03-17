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
import net.sf.hajdbc.sql.ProxyFactory;

/**
 * @author Paul Ferraro
 */
public class AllResultsCollector implements InvokeOnManyInvocationStrategy.ResultsCollector
{
	public static interface ExecutorProvider
	{
		<Z, D extends Database<Z>> ExecutorService getExecutor(DatabaseCluster<Z, D> cluster);
	}
	
	private final ExecutorProvider provider;
	
	public AllResultsCollector(ExecutorProvider provider)
	{
		this.provider = provider;
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public <Z, D extends Database<Z>, T, R, E extends Exception> Map.Entry<SortedMap<D, R>, SortedMap<D, E>> collectResults(ProxyFactory<Z, D, T, E> factory, final Invoker<Z, D, T, R, E> invoker)
	{
		DatabaseCluster<Z, D> cluster = factory.getDatabaseCluster();
		ExceptionFactory<E> exceptionFactory = factory.getExceptionFactory();
		Set<D> databaseSet = cluster.getBalancer();
		
		if (databaseSet.isEmpty())
		{
			exceptionFactory.createException(Messages.NO_ACTIVE_DATABASES.getMessage(cluster));
		}

		int size = databaseSet.size();
		
		List<Invocation<Z, D, T, R, E>> invocationList = new ArrayList<>(size);
		
		for (D database: databaseSet)
		{
			invocationList.add(new Invocation<>(invoker, database, factory.get(database)));
		}
		
		try
		{
			List<Future<R>> futureList = this.provider.getExecutor(cluster).invokeAll(invocationList);
			
			final SortedMap<D, R> resultMap = new TreeMap<>();
			final SortedMap<D, E> exceptionMap = new TreeMap<>();
			
			for (int i = 0; i < invocationList.size(); ++i)
			{
				D database = invocationList.get(i).getDatabase();
				
				try
				{
					resultMap.put(database, futureList.get(i).get());
				}
				catch (ExecutionException e)
				{
					// If this database was concurrently deactivated, just ignore the failure
					if (databaseSet.contains(database))
					{
						exceptionMap.put(database, exceptionFactory.createException(e.getCause()));
					}
				}
				catch (InterruptedException e)
				{
					Thread.currentThread().interrupt();
					
					exceptionMap.put(database, exceptionFactory.createException(e));
				}
			}
		
			return new AbstractMap.SimpleImmutableEntry<>(resultMap, exceptionMap);
		}
		catch (InterruptedException e)
		{
			throw new IllegalStateException(e);
		}
	}
	
	private static class Invocation<Z, D extends Database<Z>, T, R, E extends Exception> implements Callable<R>
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
