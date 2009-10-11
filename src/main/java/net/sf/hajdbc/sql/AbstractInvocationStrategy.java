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
import java.util.SortedMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.sf.hajdbc.Database;
import net.sf.hajdbc.DatabaseCluster;
import net.sf.hajdbc.Dialect;
import net.sf.hajdbc.ExceptionFactory;
import net.sf.hajdbc.Messages;
import net.sf.hajdbc.state.StateManager;

/**
 * @author paul
 *
 */
public abstract class AbstractInvocationStrategy<Z, D extends Database<Z>, T, R, E extends Exception> implements InvocationStrategy<Z, D, T, R, E>
{
	private static Logger logger = LoggerFactory.getLogger(AbstractInvocationStrategy.class);
	
	/**
	 * @see net.sf.hajdbc.sql.InvocationStrategy#invoke(net.sf.hajdbc.sql.SQLProxy, net.sf.hajdbc.sql.Invoker)
	 */
	@Override
	public R invoke(SQLProxy<Z, D, T, E> proxy, Invoker<Z, D, T, R, E> invoker) throws E
	{
		SortedMap<D, R> map = this.invokeAll(proxy, invoker);
		
		D masterDatabase = map.firstKey();
		R masterResult = map.get(masterDatabase);

		for (Map.Entry<D, R> entry: map.entrySet())
		{
			R result = entry.getValue();
			
			if (((masterResult != null) && (result != null)) ? !masterResult.equals(result) : (masterResult != result))
			{
				DatabaseCluster<Z, D> cluster = proxy.getDatabaseCluster();
				
				D database = entry.getKey();
				
				if (cluster.deactivate(database, cluster.getStateManager()))
				{
					logger.error(Messages.DATABASE_INCONSISTENT.getMessage(database, cluster, masterResult, result));
				}
			}
		}
		
		return masterResult;
	}
	
	protected SortedMap<D, R> invokeAll(SQLProxy<Z, D, T, E> proxy, final Invoker<Z, D, T, R, E> invoker) throws E
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
						logger.error(Messages.DATABASE_DEACTIVATED.getMessage(failedDatabase, cluster), exception);
					}
				}
			}
			
			if (!exceptionMap.isEmpty())
			{
				// If master database threw exception
				if (resultMap.isEmpty() || !exceptionMap.headMap(resultMap.firstKey()).isEmpty())
				{
					D masterDatabase = exceptionMap.firstKey();
					E masterException = exceptionMap.get(masterDatabase);
					
					// Deactivate databases with non-matching exceptions
					for (Map.Entry<D, E> entry: exceptionMap.tailMap(masterDatabase).entrySet())
					{
						E exception = entry.getValue();
						
						if (!exceptionFactory.equals(exception, masterException))
						{
							D database = entry.getKey();
							
							if (cluster.deactivate(database, stateManager))
							{
								logger.error(Messages.DATABASE_INCONSISTENT.getMessage(database, cluster, masterException, exception), exception);
							}
						}
					}
	
					// Deactivate databases with results
					for (Map.Entry<D, R> entry: resultMap.entrySet())
					{
						D database = entry.getKey();
						
						if (cluster.deactivate(database, stateManager))
						{
							logger.error(Messages.DATABASE_INCONSISTENT.getMessage(database, cluster, masterException, entry.getValue()));
						}
					}
					
					throw masterException;
				}
			}
			// Else master was successful
			// Deactivate databases with exceptions
			for (Map.Entry<D, E> entry: exceptionMap.entrySet())
			{
				D database = entry.getKey();
				E exception = entry.getValue();
				
				if (cluster.deactivate(database, stateManager))
				{
					logger.error(Messages.DATABASE_DEACTIVATED.getMessage(database, cluster), exception);
				}
			}
		}
		
		return resultMap;
	}
	
	protected abstract Map.Entry<SortedMap<D, R>, SortedMap<D, E>> collectResults(SQLProxy<Z, D, T, E> proxy, Invoker<Z, D, T, R, E> invoker);
}
