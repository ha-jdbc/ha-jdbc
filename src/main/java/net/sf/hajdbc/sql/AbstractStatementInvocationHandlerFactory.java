/*
 * HA-JDBC: High-Availablity JDBC
 * Copyright 2004-Apr 26, 2010 Paul Ferraro
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

import java.lang.reflect.InvocationHandler;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;

import net.sf.hajdbc.Database;
import net.sf.hajdbc.invocation.Invoker;

/**
 * @author paul
 *
 */
public abstract class AbstractStatementInvocationHandlerFactory<Z, D extends Database<Z>, S extends Statement> implements InvocationHandlerFactory<Z, D, Connection, S, SQLException>
{
	private final TransactionContext<Z, D> context;
	private final Class<S> statementClass;

	/**
	 * Constructs a new AbstractStatementInvocationHandlerFactory
	 * @param statementClass the class of the statement
	 * @param context the transaction context
	 */
	protected AbstractStatementInvocationHandlerFactory(Class<S> statementClass, TransactionContext<Z, D> context)
	{
		this.statementClass = statementClass;
		this.context = context;
	}
	
	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.sql.InvocationHandlerFactory#createInvocationHandler(java.lang.Object, net.sf.hajdbc.sql.SQLProxy, net.sf.hajdbc.invocation.Invoker, java.util.Map)
	 */
	@Override
	public InvocationHandler createInvocationHandler(Connection connection, SQLProxy<Z, D, Connection, SQLException> proxy, Invoker<Z, D, Connection, S, SQLException> invoker, Map<D, S> statements) throws SQLException
	{
		return this.createInvocationHandler(connection, proxy, invoker, statements, this.context, new FileSupportImpl<SQLException>(proxy.getExceptionFactory()));
	}

	protected abstract InvocationHandler createInvocationHandler(Connection connection, SQLProxy<Z, D, Connection, SQLException> proxy, Invoker<Z, D, Connection, S, SQLException> invoker, Map<D, S> statements, TransactionContext<Z, D> context, FileSupport<SQLException> fileSupport) throws SQLException;
	
	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.sql.InvocationHandlerFactory#getTargetClass()
	 */
	@Override
	public Class<S> getTargetClass()
	{
		return this.statementClass;
	}
}
