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

import java.io.PrintWriter;
import java.sql.SQLException;

import javax.naming.NamingException;
import javax.naming.Reference;
import javax.sql.PooledConnection;

import net.sf.hajdbc.sql.CommonDataSource;

/**
 * @author Paul Ferraro
 *
 */
public class ConnectionPoolDataSource extends CommonDataSource<javax.sql.ConnectionPoolDataSource, ConnectionPoolDataSourceDatabase> implements javax.sql.ConnectionPoolDataSource
{
	/**
	 * Constructs a new ConnectionPoolDataSource
	 */
	public ConnectionPoolDataSource()
	{
		super(new ConnectionPoolDataSourceFactory(), ConnectionPoolDataSourceDatabaseClusterConfiguration.class);
	}

	/**
	 * @see javax.sql.ConnectionPoolDataSource#getPooledConnection()
	 */
	@Override
	public PooledConnection getPooledConnection() throws SQLException
	{
		return this.getProxy().getPooledConnection();
	}

	/**
	 * @see javax.sql.ConnectionPoolDataSource#getPooledConnection(java.lang.String, java.lang.String)
	 */
	@Override
	public PooledConnection getPooledConnection(String user, String password) throws SQLException
	{
		return this.getProxy().getPooledConnection(user, password);
	}

	/**
	 * @see javax.sql.CommonDataSource#getLoginTimeout()
	 */
	@Override
	public int getLoginTimeout() throws SQLException
	{
		return this.getProxy().getLoginTimeout();
	}

	/**
	 * @see javax.sql.CommonDataSource#getLogWriter()
	 */
	@Override
	public PrintWriter getLogWriter() throws SQLException
	{
		return this.getProxy().getLogWriter();
	}

	/**
	 * @see javax.sql.CommonDataSource#setLoginTimeout(int)
	 */
	@Override
	public void setLoginTimeout(int timeout) throws SQLException
	{
		this.getProxy().setLoginTimeout(timeout);
	}

	/**
	 * @see javax.sql.CommonDataSource#setLogWriter(java.io.PrintWriter)
	 */
	@Override
	public void setLogWriter(PrintWriter writer) throws SQLException
	{
		this.getProxy().setLogWriter(writer);
	}

	/**
	 * @see javax.naming.Referenceable#getReference()
	 */
	@Override
	public Reference getReference() throws NamingException
	{
		return new ConnectionPoolDataSourceReference(this.getCluster(), this.getConfig());
	}
}
