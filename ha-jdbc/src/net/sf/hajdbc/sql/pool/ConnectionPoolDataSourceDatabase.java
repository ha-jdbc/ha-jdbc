/*
 * HA-JDBC: High-Availability JDBC
 * Copyright (c) 2004-2006 Paul Ferraro
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

import net.sf.hajdbc.sql.AbstractDataSourceDatabase;

/**
 * @author  Paul Ferraro
 * @version $Revision$
 * @since   1.0
 */
public class ConnectionPoolDataSourceDatabase extends AbstractDataSourceDatabase<ConnectionPoolDataSource>
{
	/**
	 * @see net.sf.hajdbc.Database#connect(T)
	 */
	public Connection connect(ConnectionPoolDataSource dataSource) throws SQLException
	{
		return this.getPooledConnection(dataSource).getConnection();
	}

	/**
	 * Returns a pooled connection from the specified connection pool data source.
	 * @param dataSource a connection pool data source
	 * @return a PooledConnection object
	 * @throws SQLException if a connection could not be obtained.
	 */
	public PooledConnection getPooledConnection(ConnectionPoolDataSource dataSource) throws SQLException
	{
		return (this.user != null) ? dataSource.getPooledConnection(this.user, this.password) : dataSource.getPooledConnection();
	}
	
	/**
	 * @see net.sf.hajdbc.sql.AbstractDataSourceDatabase#getDataSourceClass()
	 */
	protected Class<ConnectionPoolDataSource> getDataSourceClass()
	{
		return ConnectionPoolDataSource.class;
	}
}
