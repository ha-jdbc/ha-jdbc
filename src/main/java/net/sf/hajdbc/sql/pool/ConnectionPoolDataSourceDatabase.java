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

import java.sql.Connection;
import java.sql.SQLException;

import javax.sql.ConnectionPoolDataSource;
import javax.sql.PooledConnection;
import javax.xml.bind.annotation.XmlType;

import net.sf.hajdbc.management.Description;
import net.sf.hajdbc.management.MBean;
import net.sf.hajdbc.sql.CommonDataSourceDatabase;

/**
 * A database described by a {@link ConnectionPoolDataSource}.
 * @author Paul Ferraro
 */
@MBean
@Description("Database accessed via a server-side ConnectionPoolDataSource")
@XmlType(name = "database")
public class ConnectionPoolDataSourceDatabase extends CommonDataSourceDatabase<ConnectionPoolDataSource>
{
	public ConnectionPoolDataSourceDatabase()
	{
		super(ConnectionPoolDataSource.class);
	}
	
	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.Database#connect(java.lang.Object, java.lang.String)
	 */
	@Override
	public Connection connect(ConnectionPoolDataSource dataSource, String password) throws SQLException
	{
		PooledConnection connection = this.requiresAuthentication() ? dataSource.getPooledConnection(this.getUser(), password) : dataSource.getPooledConnection();
		
		return connection.getConnection();
	}
}
