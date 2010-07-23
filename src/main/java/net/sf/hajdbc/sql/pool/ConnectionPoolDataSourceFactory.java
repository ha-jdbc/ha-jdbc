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

import java.lang.reflect.InvocationHandler;

import javax.sql.ConnectionPoolDataSource;

import net.sf.hajdbc.DatabaseCluster;
import net.sf.hajdbc.sql.CommonDataSourceFactory;

/**
 * @author Paul Ferraro
 *
 */
public class ConnectionPoolDataSourceFactory extends CommonDataSourceFactory<ConnectionPoolDataSource, ConnectionPoolDataSourceDatabase>
{
	private static final long serialVersionUID = 4615188477335443494L;

	/**
	 * Constructs a new factory for creating a <code>ConnectionPoolDataSource</code>.
	 */
	public ConnectionPoolDataSourceFactory()
	{
		super(ConnectionPoolDataSource.class);
	}

	/**
	 * @see net.sf.hajdbc.sql.CommonDataSourceFactory#createInvocationHandler(net.sf.hajdbc.DatabaseCluster)
	 */
	@Override
	public InvocationHandler createInvocationHandler(DatabaseCluster<ConnectionPoolDataSource, ConnectionPoolDataSourceDatabase> cluster)
	{
		return new ConnectionPoolDataSourceInvocationHandler(cluster);
	}
}
