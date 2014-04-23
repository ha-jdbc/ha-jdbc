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
package net.sf.hajdbc.sql.pool;

import java.sql.SQLException;
import java.util.Map;

import javax.sql.PooledConnection;
import javax.sql.ConnectionPoolDataSource;

import net.sf.hajdbc.invocation.Invoker;
import net.sf.hajdbc.sql.ProxyFactory;
import net.sf.hajdbc.util.reflect.Proxies;

/**
 * 
 * @author Paul Ferraro
 */
public class PooledConnectionProxyFactory extends AbstractPooledConnectionProxyFactory<ConnectionPoolDataSource, ConnectionPoolDataSourceDatabase, PooledConnection>
{
	public PooledConnectionProxyFactory(ConnectionPoolDataSource parentProxy, ProxyFactory<ConnectionPoolDataSource, ConnectionPoolDataSourceDatabase, ConnectionPoolDataSource, SQLException> parent, Invoker<ConnectionPoolDataSource, ConnectionPoolDataSourceDatabase, ConnectionPoolDataSource, PooledConnection, SQLException> invoker, Map<ConnectionPoolDataSourceDatabase, PooledConnection> map)
	{
		super(parentProxy, parent, invoker, map);
	}

	@Override
	public PooledConnection createProxy()
	{
		return Proxies.createProxy(PooledConnection.class, new PooledConnectionInvocationHandler(this));
	}
}
