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
package net.sf.hajdbc.sql;

import java.sql.Connection;
import java.sql.SQLException;

import javax.naming.NamingException;
import javax.naming.Reference;

import net.sf.hajdbc.DatabaseClusterConfigurationFactory;

/**
 * @author Paul Ferraro
 */
public class DataSource extends CommonDataSource<javax.sql.DataSource, DataSourceDatabase> implements javax.sql.DataSource
{
	/**
	 * Constructs a new DataSource
	 */
	public DataSource()
	{
		super(new DataSourceFactory(), DataSourceDatabaseClusterConfiguration.class);
	}

	/**
	 * @see javax.sql.DataSource#getConnection()
	 */
	@Override
	public Connection getConnection() throws SQLException
	{
		return this.getProxy().getConnection();
	}

	/**
	 * @see javax.sql.DataSource#getConnection(java.lang.String, java.lang.String)
	 */
	@Override
	public Connection getConnection(String user, String password) throws SQLException
	{
		return this.getProxy().getConnection(user, password);
	}

	/**
	 * @see java.sql.Wrapper#isWrapperFor(java.lang.Class)
	 */
	@Override
	public boolean isWrapperFor(Class<?> targetClass) throws SQLException
	{
		return this.getProxy().isWrapperFor(targetClass);
	}

	/**
	 * @see java.sql.Wrapper#unwrap(java.lang.Class)
	 */
	@Override
	public <T> T unwrap(Class<T> targetClass) throws SQLException
	{
		return this.getProxy().unwrap(targetClass);
	}

	/**
	 * @see javax.naming.Referenceable#getReference()
	 */
	@Override
	public Reference getReference() throws NamingException
	{
		DatabaseClusterConfigurationFactory<javax.sql.DataSource, DataSourceDatabase> factory = this.getConfigurationFactory();
		return (factory != null) ? new DataSourceReference(this.getCluster(), factory) : new DataSourceReference(this.getCluster(), this.getConfig());
	}
}
