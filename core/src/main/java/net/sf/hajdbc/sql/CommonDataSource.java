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
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import net.sf.hajdbc.Database;
import net.sf.hajdbc.DatabaseBuilder;
import net.sf.hajdbc.DatabaseCluster;
import net.sf.hajdbc.DatabaseClusterConfigurationBuilderProvider;
import net.sf.hajdbc.DatabaseClusterConfigurationFactory;
import net.sf.hajdbc.DatabaseClusterFactory;
import net.sf.hajdbc.ExceptionType;
import net.sf.hajdbc.util.concurrent.ReferenceRegistryStoreFactory;
import net.sf.hajdbc.util.concurrent.LifecycleRegistry;
import net.sf.hajdbc.util.concurrent.Registry;
import net.sf.hajdbc.xml.XMLDatabaseClusterConfigurationFactory;

/**
 * @author Paul Ferraro
 * @param <Z> data source class
 */
public abstract class CommonDataSource<Z extends javax.sql.CommonDataSource, D extends Database<Z>, B extends DatabaseBuilder<Z, D>, F extends CommonDataSourceProxyFactory<Z, D>> implements javax.sql.CommonDataSource, CommonDataSourceProxyFactoryFactory<Z, D, F>, Registry.Factory<Void, DatabaseCluster<Z, D>, Void, SQLException>, DatabaseClusterConfigurationBuilderProvider<Z, D, B>, AutoCloseable
{
	private final Registry<Void, DatabaseCluster<Z, D>, Void, SQLException> registry = new LifecycleRegistry<>(this, new ReferenceRegistryStoreFactory(), ExceptionType.SQL.<SQLException>getExceptionFactory());
	
	private volatile Duration timeout = Duration.ofSeconds(10);
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
		this.registry.remove(null);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public DatabaseCluster<Z, D> create(Void key, Void context) throws SQLException
	{
		String cluster = this.cluster;
		if (cluster == null)
		{
			throw new SQLException();
		}
		DatabaseClusterConfigurationFactory<Z, D> configurationFactory = this.configurationFactory;
		DatabaseClusterConfigurationFactory<Z, D> factory = (configurationFactory == null) ? new XMLDatabaseClusterConfigurationFactory<>(cluster, this.config) : configurationFactory;
		return this.factory.createDatabaseCluster(cluster, factory, this.getConfigurationBuilder());
	}

	public DatabaseCluster<Z, D> getDatabaseCluster() throws SQLException
	{
		return this.registry.get(null, null);
	}
	
	public Z getProxy() throws SQLException
	{
		return this.createProxyFactory(this.getDatabaseCluster()).createProxy();
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
	 * @return the timeout
	 */
	@Override
	public Duration getTimeout()
	{
		return this.timeout;
	}

	/**
	 * @deprecated Use {@link 
	 * @param value the timeout to set, expressed in the specified units
	 * @param unit the time unit with which to qualify the specified timeout value
	 */
	@Deprecated
	public void setTimeout(long value, TimeUnit unit)
	{
		this.setTimeout(Duration.ofMillis(unit.toMillis(value)));
	}

	public void setTimeout(Duration timeout)
	{
		this.timeout = timeout;
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
