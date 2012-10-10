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
package net.sf.hajdbc.sql;

import java.lang.reflect.InvocationHandler;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Map;

import net.sf.hajdbc.Database;
import net.sf.hajdbc.invocation.Invoker;

public class PreparedStatementInvocationHandlerFactory<Z, D extends Database<Z>> extends AbstractStatementInvocationHandlerFactory<Z, D, PreparedStatement>
{
	private final String sql;

	public PreparedStatementInvocationHandlerFactory(TransactionContext<Z, D> context, String sql)
	{
		super(PreparedStatement.class, context);

		this.sql = sql;
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.sql.AbstractStatementInvocationHandlerFactory#createInvocationHandler(java.sql.Connection, net.sf.hajdbc.sql.SQLProxy, net.sf.hajdbc.invocation.Invoker, java.util.Map, net.sf.hajdbc.sql.TransactionContext, net.sf.hajdbc.sql.FileSupport)
	 */
	@Override
	protected InvocationHandler createInvocationHandler(Connection connection, SQLProxy<Z, D, Connection, SQLException> proxy, Invoker<Z, D, Connection, PreparedStatement, SQLException> invoker, Map<D, PreparedStatement> statements, TransactionContext<Z, D> context, FileSupport<SQLException> fileSupport) throws SQLException
	{
		return new PreparedStatementInvocationHandler<Z, D>(connection, proxy, invoker, statements, context, fileSupport, this.sql);
	}
}
