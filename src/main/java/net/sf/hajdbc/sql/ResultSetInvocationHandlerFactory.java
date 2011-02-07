/*
 * HA-JDBC: High-Availablity JDBC
 * Copyright 2004-Apr 28, 2010 Paul Ferraro
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
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;

import net.sf.hajdbc.Database;
import net.sf.hajdbc.invocation.Invoker;

/**
 * @author paul
 *
 */
public class ResultSetInvocationHandlerFactory<Z, D extends Database<Z>, S extends Statement> implements InvocationHandlerFactory<Z, D, S, ResultSet, SQLException>
{
	private final FileSupport<SQLException> fileSupport;
	private final TransactionContext<Z, D> transactionContext;

	public ResultSetInvocationHandlerFactory(TransactionContext<Z, D> transactionContext, FileSupport<SQLException> fileSupport)
	{
		this.fileSupport = fileSupport;
		this.transactionContext = transactionContext;
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.sql.InvocationHandlerFactory#getTargetClass()
	 */
	@Override
	public Class<ResultSet> getTargetClass()
	{
		return ResultSet.class;
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.sql.InvocationHandlerFactory#createInvocationHandler(java.lang.Object, net.sf.hajdbc.sql.SQLProxy, net.sf.hajdbc.invocation.Invoker, java.util.Map)
	 */
	@Override
	public InvocationHandler createInvocationHandler(S statement, SQLProxy<Z, D, S, SQLException> proxy, Invoker<Z, D, S, ResultSet, SQLException> invoker, Map<D, ResultSet> resultSets) throws SQLException
	{
		return new ResultSetInvocationHandler<Z, D, S>(statement, proxy, invoker, resultSets, this.transactionContext, this.fileSupport);
	}
}
