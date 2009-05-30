/*
 * HA-JDBC: High-Availability JDBC
 * Copyright (c) 2004-2008 Paul Ferraro
 * 
 * This library is free software; you can redistribute it and/or modify it 
 * under the terms of the GNU Lesser General Public License as published by the 
 * Free Software Foundation; either version 2.1 of the License, or (at your 
 * option) any later version.
 * 
 * This library is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or 
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public License 
 * for more details.
 * 
 * You should have received a copy of the GNU Lesser General Public License
 * along with this library; if not, write to the Free Software Foundation, 
 * Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA
 * 
 * Contact: ferraro@users.sourceforge.net
 */
package net.sf.hajdbc.sql.pool;

import java.sql.Connection;
import java.sql.SQLException;

import javax.sql.ConnectionPoolDataSource;
import javax.sql.PooledConnection;

import net.sf.hajdbc.sql.CommonDataSourceDatabase;

/**
 * A database described by a {@link ConnectionPoolDataSource}.
 * @author Paul Ferraro
 */
public class ConnectionPoolDataSourceDatabase extends CommonDataSourceDatabase<ConnectionPoolDataSource>
{
	/**
	 * Constructs a new database described by a {@link ConnectionPoolDataSource}.
	 */
	public ConnectionPoolDataSourceDatabase()
	{
		super(ConnectionPoolDataSource.class);
	}

	/**
	 * @see net.sf.hajdbc.Database#connect(java.lang.Object)
	 */
	@Override
	public Connection connect(ConnectionPoolDataSource dataSource) throws SQLException
	{
		String user = this.getUser();
		PooledConnection connection = (user != null) ? dataSource.getPooledConnection(user, this.getPassword()) : dataSource.getPooledConnection();
		
		return connection.getConnection();
	}
}
