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
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;

import net.sf.hajdbc.Database;

/**
 * @author paul
 *
 */
public class CallableStatementInvocationHandlerFactory<Z, D extends Database<Z>> extends AbstractStatementInvocationHandlerFactory<Z, D, CallableStatement>
{
	public CallableStatementInvocationHandlerFactory(TransactionContext<Z, D> context)
	{
		super(CallableStatement.class, context);
	}
	
	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.sql.AbstractStatementInvocationHandlerFactory#createInvocationHandler(java.sql.Connection, net.sf.hajdbc.sql.SQLProxy, net.sf.hajdbc.sql.Invoker, java.util.Map, net.sf.hajdbc.sql.TransactionContext, net.sf.hajdbc.sql.FileSupport)
	 */
	@Override
	protected InvocationHandler createInvocationHandler(Connection connection, SQLProxy<Z, D, Connection, SQLException> proxy, Invoker<Z, D, Connection, CallableStatement, SQLException> invoker, Map<D, CallableStatement> statements, TransactionContext<Z, D> context, FileSupport<SQLException> fileSupport) throws SQLException
	{
		return new CallableStatementInvocationHandler<Z, D>(connection, proxy, invoker, statements, context, fileSupport);
	}
}
