/*
 * HA-JDBC: High-Availability JDBC
 * Copyright (c) 2004-2006 Paul Ferraro
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
import java.util.NoSuchElementException;
import java.util.SortedMap;
import java.util.TreeMap;

import net.sf.hajdbc.Balancer;
import net.sf.hajdbc.Database;
import net.sf.hajdbc.DatabaseCluster;
import net.sf.hajdbc.Messages;

/**
 * @author Paul Ferraro
 *
 */
public class DatabaseReadInvocationStrategy<D, T, R> implements InvocationStrategy<D, T, R>
{
	/**
	 * @see net.sf.hajdbc.sql.InvocationStrategy#invoke(net.sf.hajdbc.sql.SQLProxy, net.sf.hajdbc.sql.Invoker)
	 */
	public R invoke(SQLProxy<D, T> proxy, Invoker<D, T, R> invoker) throws Exception
	{
		SortedMap<Database<D>, R> map = this.invokeAll(proxy, invoker);
		
		return map.get(map.firstKey());
	}
	
	protected SortedMap<Database<D>, R> invokeAll(SQLProxy<D, T> proxy, Invoker<D, T, R> invoker) throws Exception
	{
		DatabaseCluster<D> cluster = proxy.getDatabaseCluster();
		Balancer<D> balancer = cluster.getBalancer();
		
		try
		{
			while (true)
			{
				Database<D> database = balancer.next();
				T object = proxy.getObject(database);
	
				try
				{
					balancer.beforeInvocation(database);
					
					R result = invoker.invoke(database, object);
					
					SortedMap<Database<D>, R> resultMap = new TreeMap<Database<D>, R>();
					
					resultMap.put(database, result);
					
					return resultMap;
				}
				catch (SQLException e)
				{
					cluster.handleFailure(database, e);
				}
				finally
				{
					balancer.afterInvocation(database);
				}
			}
		}
		catch (NoSuchElementException e)
		{
			throw new SQLException(Messages.getMessage(Messages.NO_ACTIVE_DATABASES, cluster));
		}
	}
}
