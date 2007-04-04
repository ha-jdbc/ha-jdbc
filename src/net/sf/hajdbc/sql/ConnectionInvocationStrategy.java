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

import java.sql.Connection;

import net.sf.hajdbc.util.reflect.ProxyFactory;

/**
 * @author Paul Ferraro
 *
 */
public class ConnectionInvocationStrategy<D> extends DatabaseWriteInvocationStrategy<D, D, Connection>
{
	private D connectionFactory;
	
	/**
	 * @param executor
	 * @param lockList
	 */
	public ConnectionInvocationStrategy(D connectionFactory)
	{
		super(null);
		
		this.connectionFactory = connectionFactory;
	}

	/**
	 * @see net.sf.hajdbc.sql.DatabaseWriteInvocationStrategy#invoke(net.sf.hajdbc.sql.SQLProxy, net.sf.hajdbc.sql.Invoker)
	 */
	@Override
	public Connection invoke(SQLProxy<D, D> proxy, Invoker<D, D, Connection> invoker) throws Exception
	{
		return ProxyFactory.createProxy(Connection.class, new ConnectionInvocationHandler<D>(this.connectionFactory, proxy, invoker, this.invokeAll(proxy, invoker), new FileSupportImpl()));
	}
}
