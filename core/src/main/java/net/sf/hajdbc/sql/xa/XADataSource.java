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
package net.sf.hajdbc.sql.xa;

import java.io.PrintWriter;
import java.sql.SQLException;

import javax.sql.XAConnection;

import net.sf.hajdbc.DatabaseCluster;
import net.sf.hajdbc.SimpleDatabaseClusterConfigurationFactory;
import net.sf.hajdbc.sql.CommonDataSource;

/**
 * @author Paul Ferraro
 */
public class XADataSource extends CommonDataSource<javax.sql.XADataSource, XADataSourceDatabase, XADataSourceDatabaseBuilder, XADataSourceProxyFactory> implements javax.sql.XADataSource
{
	private final XADataSourceDatabaseClusterConfigurationBuilder builder = new XADataSourceDatabaseClusterConfigurationBuilder();

	@Override
	public XADataSourceDatabaseClusterConfigurationBuilder getConfigurationBuilder()
	{
		this.setConfigurationFactory(new SimpleDatabaseClusterConfigurationFactory<javax.sql.XADataSource, XADataSourceDatabase>());
		return this.builder;
	}

	@Override
	public XADataSourceProxyFactory createProxyFactory(DatabaseCluster<javax.sql.XADataSource, XADataSourceDatabase> cluster)
	{
		return new XADataSourceProxyFactory(cluster);
	}

	/**
	 * @see javax.sql.XADataSource#getXAConnection()
	 */
	@Override
	public XAConnection getXAConnection() throws SQLException
	{
		String user = this.getUser();
		return (user != null) ? this.getProxy().getXAConnection(user, this.getPassword()) : this.getProxy().getXAConnection();
	}

	/**
	 * @see javax.sql.XADataSource#getXAConnection(java.lang.String, java.lang.String)
	 */
	@Override
	public XAConnection getXAConnection(String user, String password) throws SQLException
	{
		return this.getProxy().getXAConnection(user, password);
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
}
