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

import javax.sql.ConnectionPoolDataSource;

import net.sf.hajdbc.DatabaseClusterConfigurationFactory;
import net.sf.hajdbc.sql.CommonDataSourceReference;

/**
 * @author Paul Ferraro
 */
public class ConnectionPoolDataSourceReference extends CommonDataSourceReference<ConnectionPoolDataSource>
{
	private static final long serialVersionUID = 2473805187473417008L;

	/**
	 * Constructs a reference to a <code>ConnectionPoolDataSource</code> for the specified cluster
	 * @param cluster a cluster identifier
	 */
	public ConnectionPoolDataSourceReference(String cluster)
	{
		this(cluster, (String) null);
	}
	
	/**
	 * Constructs a reference to a <code>ConnectionPoolDataSource</code> for the specified cluster
	 * @param cluster a cluster identifier
	 * @param config the uri of the configuration file
	 */
	public ConnectionPoolDataSourceReference(String cluster, String config)
	{
		super(ConnectionPoolDataSource.class, ConnectionPoolDataSourceFactory.class, cluster, config);
	}
	
	/**
	 * Constructs a reference to a <code>ConnectionPoolDataSource</code> for the specified cluster
	 * @param cluster
	 * @param factory
	 */
	public ConnectionPoolDataSourceReference(String cluster, DatabaseClusterConfigurationFactory<ConnectionPoolDataSource, ConnectionPoolDataSourceDatabase> factory)
	{
		super(ConnectionPoolDataSource.class, ConnectionPoolDataSourceFactory.class, cluster, factory);
	}
}
