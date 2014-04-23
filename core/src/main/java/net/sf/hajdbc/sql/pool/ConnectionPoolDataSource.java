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
package net.sf.hajdbc.sql.pool;

import java.sql.SQLException;

import javax.sql.PooledConnection;

import net.sf.hajdbc.DatabaseCluster;
import net.sf.hajdbc.SimpleDatabaseClusterConfigurationFactory;
import net.sf.hajdbc.sql.CommonDataSource;

/**
 * @author Paul Ferraro
 *
 */
public class ConnectionPoolDataSource extends CommonDataSource<javax.sql.ConnectionPoolDataSource, ConnectionPoolDataSourceDatabase, ConnectionPoolDataSourceDatabaseBuilder, ConnectionPoolDataSourceProxyFactory> implements javax.sql.ConnectionPoolDataSource
{
	private final ConnectionPoolDataSourceDatabaseClusterConfigurationBuilder builder = new ConnectionPoolDataSourceDatabaseClusterConfigurationBuilder();
	
	@Override
	public ConnectionPoolDataSourceDatabaseClusterConfigurationBuilder getConfigurationBuilder()
	{
		this.setConfigurationFactory(new SimpleDatabaseClusterConfigurationFactory<javax.sql.ConnectionPoolDataSource, ConnectionPoolDataSourceDatabase>());
		return this.builder;
	}

	@Override
	public ConnectionPoolDataSourceProxyFactory createProxyFactory(DatabaseCluster<javax.sql.ConnectionPoolDataSource, ConnectionPoolDataSourceDatabase> cluster)
	{
		return new ConnectionPoolDataSourceProxyFactory(cluster);
	}

	/**
	 * @see javax.sql.ConnectionPoolDataSource#getPooledConnection()
	 */
	@Override
	public PooledConnection getPooledConnection() throws SQLException
	{
		String user = this.getUser();
		return (user != null) ? this.getProxy().getPooledConnection(user, this.getPassword()) : this.getProxy().getPooledConnection();
	}

	/**
	 * @see javax.sql.ConnectionPoolDataSource#getPooledConnection(java.lang.String, java.lang.String)
	 */
	@Override
	public PooledConnection getPooledConnection(String user, String password) throws SQLException
	{
		return this.getProxy().getPooledConnection(user, password);
	}
}
