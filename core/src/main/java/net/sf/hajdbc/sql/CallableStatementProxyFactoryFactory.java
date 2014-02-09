/*
 * HA-JDBC: High-Availability JDBC
 * Copyright (C) 2013  Paul Ferraro
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

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;

import net.sf.hajdbc.Database;
import net.sf.hajdbc.invocation.Invoker;

/**
 * 
 * @author Paul Ferraro
 */
public class CallableStatementProxyFactoryFactory<Z, D extends Database<Z>> implements ProxyFactoryFactory<Z, D, Connection, SQLException, CallableStatement, SQLException>
{
	private final TransactionContext<Z, D> context;
	private final List<Lock> locks;
	
	public CallableStatementProxyFactoryFactory(TransactionContext<Z, D> context, List<Lock> locks)
	{
		this.context = context;
		this.locks = locks;
	}

	@Override
	public ProxyFactory<Z, D, CallableStatement, SQLException> createProxyFactory(Connection connection, ProxyFactory<Z, D, Connection, SQLException> parent, Invoker<Z, D, Connection, CallableStatement, SQLException> invoker, Map<D, CallableStatement> statements)
	{
		return new CallableStatementProxyFactory<>(connection, parent, invoker, statements, this.context, this.locks);
	}
}
