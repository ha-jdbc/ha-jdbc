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
package net.sf.hajdbc.sql;

import java.sql.SQLException;
import java.util.concurrent.TimeUnit;

import javax.naming.Referenceable;

import net.sf.hajdbc.Database;
import net.sf.hajdbc.DatabaseCluster;
import net.sf.hajdbc.DatabaseClusterConfiguration;
import net.sf.hajdbc.DatabaseClusterConfigurationFactory;
import net.sf.hajdbc.ExceptionType;
import net.sf.hajdbc.util.concurrent.ReferenceRegistryStoreFactory;
import net.sf.hajdbc.util.concurrent.Registry;
import net.sf.hajdbc.util.reflect.ProxyFactory;
import net.sf.hajdbc.xml.XMLDatabaseClusterConfigurationFactory;

/**
 * @author Paul Ferraro
 * @param <Z> data source class
 */
public abstract class CommonDataSource<Z extends javax.sql.CommonDataSource, D extends Database<Z>> implements Referenceable, javax.sql.CommonDataSource, Registry.Factory<Void, DatabaseCluster<Z, D>, Void, SQLException>
{
	private final CommonDataSourceInvocationHandlerFactory<Z, D> factory;
	private final Class<? extends DatabaseClusterConfiguration<Z, D>> configurationClass;
	
	private final Registry<Void, DatabaseCluster<Z, D>, Void, SQLException> registry = new Registry<Void, DatabaseCluster<Z, D>, Void, SQLException>(this, new ReferenceRegistryStoreFactory(), ExceptionType.getExceptionFactory(SQLException.class));
	
	private volatile long timeout = 10;
	private volatile TimeUnit timeoutUnit = TimeUnit.SECONDS;
	private volatile String cluster;
	private volatile String config;
	private volatile DatabaseClusterConfigurationFactory<Z, D> configurationFactory;	
	
	protected CommonDataSource(CommonDataSourceInvocationHandlerFactory<Z, D> factory, Class<? extends DatabaseClusterConfiguration<Z, D>> configurationClass)
	{
		this.factory = factory;
		this.configurationClass = configurationClass;
	}
	
	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.util.concurrent.Registry.Factory#create(java.lang.Object, java.lang.Object)
	 */
	@Override
	public DatabaseCluster<Z, D> create(Void key, Void context) throws SQLException
	{
		DatabaseClusterConfigurationFactory<Z, D> factory = (this.configurationFactory != null) ? this.configurationFactory : new XMLDatabaseClusterConfigurationFactory<Z, D>(this.configurationClass, this.cluster, this.config);
		
		return new DatabaseClusterImpl<Z, D>(this.cluster, factory.createConfiguration(), factory);
	}

	public Z getProxy() throws SQLException
	{
		return ProxyFactory.createProxy(this.factory.getTargetClass(), this.factory.createInvocationHandler(this.registry.get(null, null)));
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
	
	public DatabaseClusterConfigurationFactory<Z, D> getConfigurationFactory()
	{
		return this.configurationFactory;
	}
	
	public void setConfigurationFactory(DatabaseClusterConfigurationFactory<Z, D> factory)
	{
		this.configurationFactory = factory;
	}

	/**
	 * @return the timeout
	 */
	@Override
	public long getTimeout()
	{
		return this.timeout;
	}

	/**
	 * @param timeout the timeout to set
	 */
	public void setTimeout(long timeout)
	{
		this.timeout = timeout;
	}

	/**
	 * @return the timeoutUnit
	 */
	@Override
	public TimeUnit getTimeoutUnit()
	{
		return this.timeoutUnit;
	}

	/**
	 * @param timeoutUnit the timeoutUnit to set
	 */
	public void setTimeoutUnit(TimeUnit timeoutUnit)
	{
		this.timeoutUnit = timeoutUnit;
	}
}
