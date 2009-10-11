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

import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Savepoint;
import java.util.Map;

import net.sf.hajdbc.Database;
import net.sf.hajdbc.ExceptionFactory;

/**
 * @author Paul Ferraro
 * @param <D> 
 */
public class SavepointInvocationHandler<Z, D extends Database<Z>> extends AbstractChildInvocationHandler<Z, D, Connection, Savepoint, SQLException>
{
	/**
	 * @param connection the connection that created this savepoint
	 * @param proxy the invocation handler of the connection that created this savepoint
	 * @param invoker the invoker used to create this savepoint
	 * @param savepointMap a map of database to underlying savepoint
	 * @throws Exception
	 */
	protected SavepointInvocationHandler(Connection connection, SQLProxy<Z, D, Connection, SQLException> proxy, Invoker<Z, D, Connection, Savepoint, SQLException> invoker, Map<D, Savepoint> savepointMap)
	{
		super(connection, proxy, invoker, Savepoint.class, savepointMap);
	}

	/**
	 * @see net.sf.hajdbc.sql.AbstractChildInvocationHandler#getInvocationStrategy(java.lang.Object, java.lang.reflect.Method, java.lang.Object[])
	 */
	@Override
	protected InvocationStrategy<Z, D, Savepoint, ?, SQLException> getInvocationStrategy(Savepoint object, Method method, Object[] parameters)
	{
		return new DriverReadInvocationStrategy<Z, D, Savepoint, Object, SQLException>();
	}

	/**
	 * @see net.sf.hajdbc.sql.AbstractChildInvocationHandler#close(java.lang.Object, java.lang.Object)
	 */
	@Override
	protected void close(Connection connection, Savepoint savepoint) throws SQLException
	{
		connection.releaseSavepoint(savepoint);
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.sql.SQLProxy#getExceptionFactory()
	 */
	@Override
	public ExceptionFactory<SQLException> getExceptionFactory()
	{
		return SQLExceptionFactory.getInstance();
	}
}
