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
package net.sf.hajdbc.sql.xa;

import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;

import javax.sql.XAConnection;
import javax.sql.XADataSource;

import net.sf.hajdbc.durability.Durability;
import net.sf.hajdbc.invocation.InvocationStrategy;
import net.sf.hajdbc.invocation.Invoker;
import net.sf.hajdbc.sql.InvocationHandlerFactory;
import net.sf.hajdbc.sql.SQLProxy;
import net.sf.hajdbc.sql.TransactionContext;
import net.sf.hajdbc.sql.pool.AbstractPooledConnectionInvocationHandler;
import net.sf.hajdbc.util.reflect.Methods;

/**
 * @author Paul Ferraro
 */
@SuppressWarnings("nls")
public class XAConnectionInvocationHandler extends AbstractPooledConnectionInvocationHandler<XADataSource, XADataSourceDatabase, XAConnection>
{
	private static final TransactionContext<XADataSource, XADataSourceDatabase> transactionContext = new XATransactionContext();
	
	private static final Method getXAResource = Methods.getMethod(XAConnection.class, "getXAResource");
	
	/**
	 * @param dataSource
	 * @param proxy
	 * @param invoker
	 * @param objectMap
	 * @throws Exception
	 */
	public XAConnectionInvocationHandler(XADataSource dataSource, SQLProxy<XADataSource, XADataSourceDatabase, XADataSource, SQLException> proxy, Invoker<XADataSource, XADataSourceDatabase, XADataSource, XAConnection, SQLException> invoker, Map<XADataSourceDatabase, XAConnection> objectMap)
	{
		super(dataSource, proxy, invoker, XAConnection.class, objectMap);
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.sql.AbstractInvocationHandler#getInvocationHandlerFactory(java.lang.Object, java.lang.reflect.Method, java.lang.Object[])
	 */
	@Override
	protected InvocationHandlerFactory<XADataSource, XADataSourceDatabase, XAConnection, ?, SQLException> getInvocationHandlerFactory(XAConnection object, Method method, Object[] parameters) throws SQLException
	{
		if (method.equals(getXAResource))
		{
			return new XAResourceInvocationHandlerFactory();
		}
		
		return super.getInvocationHandlerFactory(object, method, parameters);
	}

	/**
	 * @see net.sf.hajdbc.sql.pool.AbstractPooledConnectionInvocationHandler#createTransactionContext()
	 */
	@Override
	protected TransactionContext<XADataSource, XADataSourceDatabase> createTransactionContext()
	{
		return transactionContext;
	}
	
	static class XATransactionContext implements TransactionContext<XADataSource, XADataSourceDatabase>
	{
		@Override
		public void close()
		{
			// Nothing to close
		}

		@Override
		public InvocationStrategy start(InvocationStrategy strategy, Connection connection) throws SQLException
		{
			return strategy;
		}

		@Override
		public <T, R> Invoker<XADataSource, XADataSourceDatabase, T, R, SQLException> start(Invoker<XADataSource, XADataSourceDatabase, T, R, SQLException> invoker, Connection connection) throws SQLException
		{
			return invoker;
		}

		@Override
		public InvocationStrategy end(InvocationStrategy strategy, Durability.Phase phase) throws SQLException
		{
			throw new IllegalStateException();
		}

		@Override
		public <T, R> Invoker<XADataSource, XADataSourceDatabase, T, R, SQLException> end(Invoker<XADataSource, XADataSourceDatabase, T, R, SQLException> invoker, Durability.Phase phase) throws SQLException
		{
			throw new IllegalStateException();
		}
	}
}
