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

import javax.sql.ConnectionPoolDataSource;

import net.sf.hajdbc.DatabaseCluster;
import net.sf.hajdbc.sql.CommonDataSourceProxyFactory;
import net.sf.hajdbc.util.reflect.Proxies;

/**
 * 
 * @author Paul Ferraro
 */
public class ConnectionPoolDataSourceProxyFactory extends CommonDataSourceProxyFactory<ConnectionPoolDataSource, ConnectionPoolDataSourceDatabase>
{
	public ConnectionPoolDataSourceProxyFactory(DatabaseCluster<ConnectionPoolDataSource, ConnectionPoolDataSourceDatabase> cluster)
	{
		super(cluster);
	}

	@Override
	public ConnectionPoolDataSource createProxy()
	{
		ConnectionPoolDataSource ds = Proxies.createProxy(ConnectionPoolDataSource.class, new ConnectionPoolDataSourceInvocationHandler(this));
		getDatabaseCluster().addListener(ds, this);
		return ds;
	}
}
