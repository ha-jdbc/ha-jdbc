/*
 * HA-JDBC: High-Availability JDBC
 * Copyright (c) 2004-2008 Paul Ferraro
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
package net.sf.hajdbc.sql.xa;

import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;

import javax.sql.XAConnection;
import javax.sql.XADataSource;

import net.sf.hajdbc.Database;
import net.sf.hajdbc.sql.InvocationStrategy;
import net.sf.hajdbc.sql.Invoker;
import net.sf.hajdbc.sql.SQLProxy;
import net.sf.hajdbc.sql.TransactionContext;
import net.sf.hajdbc.sql.pool.AbstractPooledConnectionInvocationHandler;
import net.sf.hajdbc.util.reflect.Methods;

/**
 * @author Paul Ferraro
 */
@SuppressWarnings("nls")
public class XAConnectionInvocationHandler extends AbstractPooledConnectionInvocationHandler<XADataSource, XAConnection>
{
	private static final Method getXAResource = Methods.getMethod(XAConnection.class, "getXAResource");
	
	/**
	 * @param dataSource
	 * @param proxy
	 * @param invoker
	 * @param objectMap
	 * @throws Exception
	 */
	public XAConnectionInvocationHandler(XADataSource dataSource, SQLProxy<XADataSource, XADataSource> proxy, Invoker<XADataSource, XADataSource, XAConnection> invoker, Map<Database<XADataSource>, XAConnection> objectMap) throws Exception
	{
		super(dataSource, proxy, invoker, XAConnection.class, objectMap);
	}

	@Override
	protected InvocationStrategy<XADataSource, XAConnection, ?> getInvocationStrategy(XAConnection connection, Method method, Object[] parameters) throws Exception
	{
		if (method.equals(getXAResource))
		{
			return new XAResourceInvocationStrategy(this.cluster, connection);
		}
		
		return super.getInvocationStrategy(connection, method, parameters);
	}

	/**
	 * @see net.sf.hajdbc.sql.pool.AbstractPooledConnectionInvocationHandler#createTransactionContext()
	 */
	@Override
	protected TransactionContext<XADataSource> createTransactionContext()
	{
		return new TransactionContext<XADataSource>()
		{
			@Override
			public void close()
			{
				// Do nothing
			}

			@Override
			public <T, R> InvocationStrategy<XADataSource, T, R> end(InvocationStrategy<XADataSource, T, R> strategy) throws SQLException
			{
				return strategy;
			}

			@Override
			public <T, R> InvocationStrategy<XADataSource, T, R> start(InvocationStrategy<XADataSource, T, R> strategy, Connection connection) throws SQLException
			{
				return strategy;
			}
		};
	}
}
