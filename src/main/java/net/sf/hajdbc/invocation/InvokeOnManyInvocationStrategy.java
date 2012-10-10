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
import java.util.SortedMap;

import net.sf.hajdbc.Database;
import net.sf.hajdbc.DatabaseCluster;
import net.sf.hajdbc.ExceptionFactory;
import net.sf.hajdbc.Messages;
import net.sf.hajdbc.dialect.Dialect;
import net.sf.hajdbc.logging.Level;
import net.sf.hajdbc.logging.Logger;
import net.sf.hajdbc.logging.LoggerFactory;
import net.sf.hajdbc.sql.SQLProxy;
import net.sf.hajdbc.state.StateManager;

/**
 * @author paul
 *
 */
public abstract class InvokeOnManyInvocationStrategy implements InvocationStrategy
{
	private static Logger logger = LoggerFactory.getLogger(InvokeOnManyInvocationStrategy.class);
	
	/**
	 * @see net.sf.hajdbc.invocation.InvocationStrategy#invoke(net.sf.hajdbc.sql.SQLProxy, net.sf.hajdbc.invocation.Invoker)
	 */
	@Override
	public <Z, D extends Database<Z>, T, R, E extends Exception> SortedMap<D, R> invoke(SQLProxy<Z, D, T, E> proxy, Invoker<Z, D, T, R, E> invoker) throws E
	{
		Map.Entry<SortedMap<D, R>, SortedMap<D, E>> results = this.collectResults(proxy, invoker);
		
		SortedMap<D, R> resultMap = results.getKey();
		SortedMap<D, E> exceptionMap = results.getValue();
		
		if (!exceptionMap.isEmpty())
		{
			ExceptionFactory<E> exceptionFactory = proxy.getExceptionFactory();
			DatabaseCluster<Z, D> cluster = proxy.getDatabaseCluster();
			Dialect dialect = cluster.getDialect();
			
			List<D> failedDatabases = new ArrayList<D>(exceptionMap.size());
			
			// Determine which exceptions are due to failures
			for (Map.Entry<D, E> entry: exceptionMap.entrySet())
			{
				if (exceptionFactory.indicatesFailure(entry.getValue(), dialect))
				{
					failedDatabases.add(entry.getKey());
				}
			}

			StateManager stateManager = cluster.getStateManager();
			
			// Deactivate failed databases, unless all failed
			if (!resultMap.isEmpty() || (failedDatabases.size() < exceptionMap.size()))
			{
				for (D failedDatabase: failedDatabases)
				{
					E exception = exceptionMap.remove(failedDatabase);
					
					if (cluster.deactivate(failedDatabase, stateManager))
					{
						logger.log(Level.ERROR, exception, Messages.DATABASE_DEACTIVATED.getMessage(), failedDatabase, cluster);
					}
				}
			}
			
			if (!exceptionMap.isEmpty())
			{
				// If primary database threw exception
				if (resultMap.isEmpty() || !exceptionMap.headMap(resultMap.firstKey()).isEmpty())
				{
					D primaryDatabase = exceptionMap.firstKey();
					E primaryException = exceptionMap.get(primaryDatabase);
					
					// Deactivate databases with non-matching exceptions
					for (Map.Entry<D, E> entry: exceptionMap.tailMap(primaryDatabase).entrySet())
					{
						E exception = entry.getValue();
						
						if (!exceptionFactory.equals(exception, primaryException))
						{
							D database = entry.getKey();
							
							if (cluster.deactivate(database, stateManager))
							{
								logger.log(Level.ERROR, exception, Messages.DATABASE_INCONSISTENT.getMessage(), database, cluster, primaryException, exception);
							}
						}
					}
	
					// Deactivate databases with results
					for (Map.Entry<D, R> entry: resultMap.entrySet())
					{
						D database = entry.getKey();
						
						if (cluster.deactivate(database, stateManager))
						{
							logger.log(Level.ERROR, Messages.DATABASE_INCONSISTENT.getMessage(), database, cluster, primaryException, entry.getValue());
						}
					}
					
					throw primaryException;
				}
			}
			// Else primary was successful
			// Deactivate databases with exceptions
			for (Map.Entry<D, E> entry: exceptionMap.entrySet())
			{
				D database = entry.getKey();
				E exception = entry.getValue();
				
				if (cluster.deactivate(database, stateManager))
				{
					logger.log(Level.ERROR, exception, Messages.DATABASE_DEACTIVATED.getMessage(), database, cluster);
				}
			}
		}
		
		return resultMap;
	}
	
	protected abstract <Z, D extends Database<Z>, T, R, E extends Exception> Map.Entry<SortedMap<D, R>, SortedMap<D, E>> collectResults(SQLProxy<Z, D, T, E> proxy, Invoker<Z, D, T, R, E> invoker);
}
