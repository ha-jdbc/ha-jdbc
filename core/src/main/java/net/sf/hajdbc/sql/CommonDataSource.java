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

import java.io.PrintWriter;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.AbstractMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Logger;

import net.sf.hajdbc.Database;
import net.sf.hajdbc.DatabaseBuilder;
import net.sf.hajdbc.DatabaseCluster;
import net.sf.hajdbc.DatabaseClusterConfigurationBuilderProvider;
import net.sf.hajdbc.DatabaseClusterConfigurationFactory;
import net.sf.hajdbc.DatabaseClusterFactory;
import net.sf.hajdbc.xml.XMLDatabaseClusterConfigurationFactory;

/**
 * @author Paul Ferraro
 * @param <Z> data source class
 */
public abstract class CommonDataSource<Z extends javax.sql.CommonDataSource, D extends Database<Z>, B extends DatabaseBuilder<Z, D>, F extends CommonDataSourceProxyFactory<Z, D>> implements javax.sql.CommonDataSource, CommonDataSourceProxyFactoryFactory<Z, D, F>, DatabaseClusterConfigurationBuilderProvider<Z, D, B>, AutoCloseable
{
	private final AtomicReference<Map.Entry<F, Z>> reference = new AtomicReference<>();
	
	private volatile String cluster;
	private volatile String config;
	private volatile String user;
	private volatile String password;
	private volatile DatabaseClusterFactory<Z, D> factory = new DatabaseClusterFactoryImpl<>();
	private volatile DatabaseClusterConfigurationFactory<Z, D> configurationFactory;	

	@Deprecated
	public void stop() throws SQLException
	{
		this.close();
	}

	@Override
	public void close() throws SQLException
	{
		Map.Entry<F, Z> entry = this.reference.getAndSet(null);
		if (entry != null)
		{
			entry.getKey().close();
			entry.getKey().getDatabaseCluster().stop();
		}
	}

	public Z getProxy() throws SQLException
	{
		Map.Entry<F, Z> entry = this.reference.get();
		if (entry == null) {
			String id = this.cluster;
			if (id == null)
			{
				throw new SQLException();
			}
			DatabaseClusterConfigurationFactory<Z, D> configurationFactory = this.configurationFactory;
			DatabaseClusterConfigurationFactory<Z, D> factory = (configurationFactory == null) ? new XMLDatabaseClusterConfigurationFactory<>(id, this.config) : configurationFactory;
			DatabaseCluster<Z, D> cluster = this.factory.createDatabaseCluster(id, factory, this.getConfigurationBuilder());
			cluster.start();
			@SuppressWarnings("resource")
			F proxyFactory = this.createProxyFactory(cluster);
			entry = new AbstractMap.SimpleImmutableEntry<>(proxyFactory, proxyFactory.createProxy());
			if (!this.reference.compareAndSet(null, entry))
			{
				proxyFactory.close();
				cluster.stop();
				return this.getProxy();
			}
		}
		return entry.getValue();
	}
	
	/**
	 * @return the cluster
	 */
	public String getCluster()
	{
		return this.cluster;
	}

	/**
	 * @param cluster the cluster to set
	 */
	public void setCluster(String cluster)
	{
		this.cluster = cluster;
	}

	/**
	 * @return the config
	 */
	public String getConfig()
	{
		return this.config;
	}

	/**
	 * @param config the config to set
	 */
	public void setConfig(String config)
	{
		this.config = config;
	}
	
	public String getUser()
	{
		return this.user;
	}
	
	public void setUser(String user)
	{
		this.user = user;
	}
	
	public String getPassword()
	{
		return this.password;
	}
	
	public void setPassword(String password)
	{
		this.password = password;
	}
	
	public DatabaseClusterConfigurationFactory<Z, D> getConfigurationFactory()
	{
		return this.configurationFactory;
	}
	
	public void setConfigurationFactory(DatabaseClusterConfigurationFactory<Z, D> configurationFactory)
	{
		this.configurationFactory = configurationFactory;
	}
	
	public DatabaseClusterFactory<Z, D> getFactory()
	{
		return this.factory;
	}
	
	public void setFactory(DatabaseClusterFactory<Z, D> clusterFactory)
	{
		this.factory = clusterFactory;
	}

	/**
	 * @throws SQLFeatureNotSupportedException 
	 * @see javax.sql.CommonDataSource#getParentLogger()
	 */
	@Override
	public Logger getParentLogger() throws SQLFeatureNotSupportedException
	{
		try
		{
			return this.getProxy().getParentLogger();
		}
		catch (SQLFeatureNotSupportedException e)
		{
			throw e;
		}
		catch (SQLException e)
		{
			throw new SQLFeatureNotSupportedException(e.getMessage(), e.getSQLState(), e.getErrorCode(), e.getCause());
		}
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
