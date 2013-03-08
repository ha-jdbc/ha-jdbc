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

import java.util.SortedMap;
import java.util.TreeMap;

import net.sf.hajdbc.Database;
import net.sf.hajdbc.DatabaseCluster;
import net.sf.hajdbc.ExceptionFactory;
import net.sf.hajdbc.Messages;
import net.sf.hajdbc.balancer.Balancer;
import net.sf.hajdbc.dialect.Dialect;
import net.sf.hajdbc.logging.Level;
import net.sf.hajdbc.logging.Logger;
import net.sf.hajdbc.logging.LoggerFactory;
import net.sf.hajdbc.sql.ProxyFactory;
import net.sf.hajdbc.state.StateManager;

/**
 * @author paul
 *
 */
public class InvokeOnOneInvocationStrategy implements InvocationStrategy
{
	private static Logger logger = LoggerFactory.getLogger(InvokeOnOneInvocationStrategy.class);
	
	public static interface DatabaseSelector
	{
		<Z, D extends Database<Z>> D selectDatabase(Balancer<Z, D> balancer);
	}

	private final DatabaseSelector selector;
	
	public InvokeOnOneInvocationStrategy(DatabaseSelector selector)
	{
		this.selector = selector;
	}
	
	/**
	 * @see net.sf.hajdbc.invocation.InvocationStrategy#invoke(net.sf.hajdbc.sql.SQLProxy, net.sf.hajdbc.invocation.Invoker)
	 */
	@Override
	public <Z, D extends Database<Z>, T, R, E extends Exception> SortedMap<D, R> invoke(ProxyFactory<Z, D, T, E> map, Invoker<Z, D, T, R, E> invoker) throws E
	{
		DatabaseCluster<Z, D> cluster = map.getRoot().getDatabaseCluster();
		ExceptionFactory<E> exceptionFactory = map.getExceptionFactory();
		Balancer<Z, D> balancer = cluster.getBalancer();
		Dialect dialect = cluster.getDialect();
		StateManager stateManager = cluster.getStateManager();
		
		while (true)
		{
			D database = this.selector.selectDatabase(balancer);
			
			if (database == null)
			{
				throw exceptionFactory.createException(Messages.NO_ACTIVE_DATABASES.getMessage(cluster));
			}
			
			T object = map.get(database);
			
			try
			{
				R result = balancer.invoke(invoker, database, object);
				
				SortedMap<D, R> resultMap = new TreeMap<D, R>();
				resultMap.put(database, result);
				return resultMap;
			}
			catch (Exception e)
			{
				E exception = exceptionFactory.createException(e);
				
				if (exceptionFactory.indicatesFailure(exception, dialect))
				{
					if (cluster.deactivate(database, stateManager))
					{
						logger.log(Level.ERROR, exception, Messages.DATABASE_DEACTIVATED.getMessage(), database, cluster);
					}
				}
				else
				{
					throw exception;
				}
			}
		}
	}
}
