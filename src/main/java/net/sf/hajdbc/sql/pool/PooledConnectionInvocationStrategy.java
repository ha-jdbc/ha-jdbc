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

import javax.sql.ConnectionPoolDataSource;
import javax.sql.PooledConnection;

import net.sf.hajdbc.DatabaseCluster;
import net.sf.hajdbc.sql.DatabaseWriteInvocationStrategy;
import net.sf.hajdbc.sql.Invoker;
import net.sf.hajdbc.sql.SQLProxy;
import net.sf.hajdbc.util.reflect.ProxyFactory;

/**
 * @author Paul Ferraro
 */
public class PooledConnectionInvocationStrategy extends DatabaseWriteInvocationStrategy<ConnectionPoolDataSource, ConnectionPoolDataSourceDatabase, ConnectionPoolDataSource, PooledConnection, SQLException>
{
	private ConnectionPoolDataSource dataSource;
	
	/**
	 * @param cluster 
	 * @param dataSource
	 */
	public PooledConnectionInvocationStrategy(DatabaseCluster<ConnectionPoolDataSource, ConnectionPoolDataSourceDatabase> cluster, ConnectionPoolDataSource dataSource)
	{
		super(cluster.getNonTransactionalExecutor());
		
		this.dataSource = dataSource;
	}
	
	@Override
	public PooledConnection invoke(SQLProxy<ConnectionPoolDataSource, ConnectionPoolDataSourceDatabase, ConnectionPoolDataSource, SQLException> proxy, Invoker<ConnectionPoolDataSource, ConnectionPoolDataSourceDatabase, ConnectionPoolDataSource, PooledConnection, SQLException> invoker) throws SQLException
	{
		return ProxyFactory.createProxy(PooledConnection.class, new PooledConnectionInvocationHandler(this.dataSource, proxy, invoker, this.invokeAll(proxy, invoker)));
	}
}
