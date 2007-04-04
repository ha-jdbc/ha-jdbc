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
import java.sql.Statement;

import net.sf.hajdbc.util.reflect.ProxyFactory;

/**
 * @author Paul Ferraro
 *
 */
public class StatementInvocationStrategy<D> extends DriverWriteInvocationStrategy<D, Connection, Statement>
{
	private Connection connection;
	private FileSupport fileSupport;
	
	public StatementInvocationStrategy(Connection connection, FileSupport fileSupport)
	{
		this.connection = connection;
		this.fileSupport = fileSupport;
	}
	
	/**
	 * @see net.sf.hajdbc.sql.DriverWriteInvocationStrategy#invoke(net.sf.hajdbc.sql.SQLProxy, net.sf.hajdbc.sql.Invoker)
	 */
	@Override
	public Statement invoke(SQLProxy<D, Connection> proxy, Invoker<D, Connection, Statement> invoker) throws Exception
	{
		return ProxyFactory.createProxy(java.sql.Statement.class, new StatementInvocationHandler<D>(this.connection, proxy, invoker, this.invokeAll(proxy, invoker), this.fileSupport));
	}
}
