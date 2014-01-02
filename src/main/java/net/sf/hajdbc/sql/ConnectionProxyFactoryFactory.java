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

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;

import net.sf.hajdbc.Database;
import net.sf.hajdbc.invocation.Invoker;

/**
 * 
 * @author Paul Ferraro
 */
public class ConnectionProxyFactoryFactory<Z, D extends Database<Z>, P> implements ProxyFactoryFactory<Z, D, P, SQLException, Connection, SQLException>
{
	private final TransactionContext<Z, D> context;
	
	public ConnectionProxyFactoryFactory(TransactionContext<Z, D> context)
	{
		this.context = context;
	}
	
	@Override
	public ProxyFactory<Z, D, Connection, SQLException> createProxyFactory(P parentProxy, ProxyFactory<Z, D, P, SQLException> parent, Invoker<Z, D, P, Connection, SQLException> invoker, Map<D, Connection> connections)
	{
		return new ConnectionProxyFactory<>(parentProxy, parent, invoker, connections, this.context);
	}
}
