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

import java.lang.reflect.InvocationHandler;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;

import net.sf.hajdbc.Database;

/**
 * @author Paul Ferraro
 * @param <D> 
 */
public class StatementInvocationHandlerFactory<Z, D extends Database<Z>> extends AbstractStatementInvocationHandlerFactory<Z, D, Statement>
{
	/**
	 * @param connection
	 * @param transactionContext
	 */
	public StatementInvocationHandlerFactory(TransactionContext<Z, D> context)
	{
		super(Statement.class, context);
	}
	
	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.sql.AbstractStatementInvocationHandlerFactory#createInvocationHandler(java.sql.Connection, net.sf.hajdbc.sql.SQLProxy, net.sf.hajdbc.sql.Invoker, java.util.Map, net.sf.hajdbc.sql.TransactionContext, net.sf.hajdbc.sql.FileSupport)
	 */
	@Override
	protected InvocationHandler createInvocationHandler(Connection connection, SQLProxy<Z, D, Connection, SQLException> proxy, Invoker<Z, D, Connection, Statement, SQLException> invoker, Map<D, Statement> statements, TransactionContext<Z, D> context, FileSupport<SQLException> fileSupport) throws SQLException
	{
		return new StatementInvocationHandler<Z, D>(connection, proxy, invoker, statements, context, fileSupport);
	}
}
