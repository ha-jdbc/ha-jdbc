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
package net.sf.hajdbc.sql.pool;

import java.sql.SQLException;
import java.util.Map;

import javax.sql.ConnectionPoolDataSource;
import javax.sql.PooledConnection;

import net.sf.hajdbc.sql.Invoker;
import net.sf.hajdbc.sql.LocalTransactionContext;
import net.sf.hajdbc.sql.SQLProxy;
import net.sf.hajdbc.sql.TransactionContext;

/**
 * @author Paul Ferraro
 *
 */
public class PooledConnectionInvocationHandler extends AbstractPooledConnectionInvocationHandler<ConnectionPoolDataSource, ConnectionPoolDataSourceDatabase, PooledConnection>
{
	/**
	 * @param dataSource
	 * @param proxy
	 * @param invoker
	 * @param objectMap
	 * @throws Exception
	 */
	protected PooledConnectionInvocationHandler(ConnectionPoolDataSource dataSource, SQLProxy<ConnectionPoolDataSource, ConnectionPoolDataSourceDatabase, ConnectionPoolDataSource, SQLException> proxy, Invoker<ConnectionPoolDataSource,ConnectionPoolDataSourceDatabase, ConnectionPoolDataSource, PooledConnection, SQLException> invoker, Map<ConnectionPoolDataSourceDatabase, PooledConnection> objectMap)
	{
		super(dataSource, proxy, invoker, PooledConnection.class, objectMap);
	}

	/**
	 * @see net.sf.hajdbc.sql.pool.AbstractPooledConnectionInvocationHandler#createTransactionContext()
	 */
	@Override
	protected TransactionContext<ConnectionPoolDataSource, ConnectionPoolDataSourceDatabase> createTransactionContext()
	{
		return new LocalTransactionContext<ConnectionPoolDataSource, ConnectionPoolDataSourceDatabase>(this.cluster);
	}
}
