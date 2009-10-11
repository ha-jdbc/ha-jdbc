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

import java.util.Map;

import net.sf.hajdbc.Database;
import net.sf.hajdbc.DatabaseCluster;
import net.sf.hajdbc.Dialect;
import net.sf.hajdbc.ExceptionFactory;
import net.sf.hajdbc.Messages;
import net.sf.hajdbc.state.StateManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Paul Ferraro
 * @param <D> 
 * @param <T> 
 * @param <R> 
 */
public class DriverReadInvocationStrategy<Z, D extends Database<Z>, T, R, E extends Exception> extends DatabaseReadInvocationStrategy<Z, D, T, R, E>
{
	private static Logger logger = LoggerFactory.getLogger(DriverWriteInvocationStrategy.class);

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.sql.InvocationStrategy#invoke(net.sf.hajdbc.sql.SQLProxy, net.sf.hajdbc.sql.Invoker)
	 */
	@Override
	public R invoke(SQLProxy<Z, D, T, E> proxy, Invoker<Z, D, T, R, E> invoker) throws E
	{
		DatabaseCluster<Z, D> cluster = proxy.getDatabaseCluster();
		Dialect dialect = cluster.getDialect();
		StateManager stateManager = cluster.getStateManager();
		ExceptionFactory<E> exceptionFactory = proxy.getExceptionFactory();

		for (Map.Entry<D, T> entry: proxy.entries())
		{
			D database = entry.getKey();
			
			try
			{
				return invoker.invoke(database, entry.getValue());
			}
			catch (Exception e)
			{
				E exception = exceptionFactory.createException(e);
				
				if (exceptionFactory.indicatesFailure(exception, dialect) && (cluster.getBalancer().size() > 1))
				{
					if (cluster.deactivate(database, stateManager))
					{
						logger.error(Messages.DATABASE_DEACTIVATED.getMessage(database, cluster), exception);
					}
				}
				else
				{
					throw exception;
				}
			}
		}
		
		return super.invoke(proxy, invoker);
	}
}
