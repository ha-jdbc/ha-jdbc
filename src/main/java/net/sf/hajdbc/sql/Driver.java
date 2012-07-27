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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.util.Map;
import java.util.Properties;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import net.sf.hajdbc.AbstractDriver;
import net.sf.hajdbc.DatabaseCluster;
import net.sf.hajdbc.DatabaseClusterConfigurationFactory;
import net.sf.hajdbc.DatabaseClusterFactory;
import net.sf.hajdbc.ExceptionType;
import net.sf.hajdbc.invocation.InvocationStrategyEnum;
import net.sf.hajdbc.invocation.Invoker;
import net.sf.hajdbc.util.concurrent.MapRegistryStoreFactory;
import net.sf.hajdbc.util.concurrent.LifecycleRegistry;
import net.sf.hajdbc.util.concurrent.Registry;
import net.sf.hajdbc.util.reflect.ProxyFactory;
import net.sf.hajdbc.xml.XMLDatabaseClusterConfigurationFactory;

/**
 * @author  Paul Ferraro
 */
public final class Driver extends AbstractDriver implements Registry.Factory<String, DatabaseCluster<java.sql.Driver, DriverDatabase>, Properties, SQLException>
{
	private static final Pattern URL_PATTERN = Pattern.compile("jdbc:ha-jdbc:(?://)?([^/]+)(?:/.+)?"); //$NON-NLS-1$
	private static final String CONFIG = "config"; //$NON-NLS-1$

	static
	{
		try
		{
			DriverManager.registerDriver(new Driver());
		}
		catch (SQLException e)
		{
			throw new IllegalStateException(e);
		}
	}

	private volatile DatabaseClusterFactory<java.sql.Driver, DriverDatabase> factory = new DatabaseClusterFactoryImpl<java.sql.Driver, DriverDatabase>();
	private volatile long timeout = 10;
	private volatile TimeUnit timeoutUnit = TimeUnit.SECONDS;
	
	private final Map<String, DatabaseClusterConfigurationFactory<java.sql.Driver, DriverDatabase>> configurationFactories = new ConcurrentHashMap<String, DatabaseClusterConfigurationFactory<java.sql.Driver, DriverDatabase>>();
	private final Registry<String, DatabaseCluster<java.sql.Driver, DriverDatabase>, Properties, SQLException> registry = new LifecycleRegistry<String, DatabaseCluster<java.sql.Driver, DriverDatabase>, Properties, SQLException>(this, new MapRegistryStoreFactory<String>(), ExceptionType.getExceptionFactory(SQLException.class));
	
	public void stop(String id) throws SQLException
	{
		this.registry.remove(id);
	}

	public void setFactory(DatabaseClusterFactory<java.sql.Driver, DriverDatabase> factory)
	{
		this.factory = factory;
	}
	
	/**
	 * Set custom configuration factories per cluster.
	 * @param factories a map of configuration factories per cluster identifier.
	 */
	public Map<String, DatabaseClusterConfigurationFactory<java.sql.Driver, DriverDatabase>> getConfigurationFactories()
	{
		return this.configurationFactories;
	}
	
	public void setTimeout(long value, TimeUnit unit)
	{
		this.timeout = value;
		this.timeoutUnit = unit;
	}
	
	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.AbstractDriver#getUrlPattern()
	 */
	@Override
	protected Pattern getUrlPattern()
	{
		return URL_PATTERN;
	}

	/**
	 * {@inheritDoc}
	 * @see java.sql.Driver#connect(java.lang.String, java.util.Properties)
	 */
	@Override
	public Connection connect(String url, final Properties properties) throws SQLException
	{
		String id = this.parse(url);
		
		// JDBC spec compliance
		if (id == null) return null;
		
		DatabaseCluster<java.sql.Driver, DriverDatabase> cluster = this.registry.get(id, properties);
		
		DriverInvocationHandler handler = new DriverInvocationHandler(cluster);
		
		java.sql.Driver driver = ProxyFactory.createProxy(java.sql.Driver.class, handler);
		
		Invoker<java.sql.Driver, DriverDatabase, java.sql.Driver, Connection, SQLException> invoker = new Invoker<java.sql.Driver, DriverDatabase, java.sql.Driver, Connection, SQLException>()
		{
			@Override
			public Connection invoke(DriverDatabase database, java.sql.Driver driver) throws SQLException
			{
				return driver.connect(database.getName(), properties);
			}
		};
		
		SortedMap<DriverDatabase, Connection> results = InvocationStrategyEnum.INVOKE_ON_ALL.invoke(handler, invoker);
		
		TransactionContext<java.sql.Driver, DriverDatabase> context = new LocalTransactionContext<java.sql.Driver, DriverDatabase>(cluster);
		
		InvocationHandlerFactory<java.sql.Driver, DriverDatabase, java.sql.Driver, Connection, SQLException> handlerFactory = new ConnectionInvocationHandlerFactory<java.sql.Driver, DriverDatabase, java.sql.Driver>(context);
		
		InvocationResultFactory<java.sql.Driver, DriverDatabase, Connection, SQLException> resultFactory = handler.new ProxyInvocationResultFactory<Connection>(handlerFactory, driver, invoker);
		
		return resultFactory.createResult(results);
	}
	
	/**
	 * {@inheritDoc}
	 * @see Driver#getPropertyInfo(java.lang.String, java.util.Properties)
	 */
	@Override
	public DriverPropertyInfo[] getPropertyInfo(String url, final Properties properties) throws SQLException
	{
		String id = this.parse(url);
		
		// JDBC spec compliance
		if (id == null) return null;
		
		DatabaseCluster<java.sql.Driver, DriverDatabase> cluster = this.registry.get(id, properties);
		
		DriverInvocationHandler handler = new DriverInvocationHandler(cluster);
		
		Invoker<java.sql.Driver, DriverDatabase, java.sql.Driver, DriverPropertyInfo[], SQLException> invoker = new Invoker<java.sql.Driver, DriverDatabase, java.sql.Driver, DriverPropertyInfo[], SQLException>()
		{
			@Override
			public DriverPropertyInfo[] invoke(DriverDatabase database, java.sql.Driver driver) throws SQLException
			{
				return driver.getPropertyInfo(database.getName(), properties);
			}			
		};
		
		SortedMap<DriverDatabase, DriverPropertyInfo[]> results = InvocationStrategyEnum.INVOKE_ON_NEXT.invoke(handler, invoker);

		InvocationResultFactory<java.sql.Driver, DriverDatabase, DriverPropertyInfo[], SQLException> resultFactory = handler.new SimpleInvocationResultFactory<DriverPropertyInfo[]>();
		
		return resultFactory.createResult(results);
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.util.concurrent.LifecycleRegistry.Factory#create(java.lang.Object, java.lang.Object)
	 */
	@Override
	public DatabaseCluster<java.sql.Driver, DriverDatabase> create(String id, Properties properties) throws SQLException
	{
		DatabaseClusterConfigurationFactory<java.sql.Driver, DriverDatabase> factory = this.configurationFactories.get(id);
		
		if (factory == null)
		{
			factory = new XMLDatabaseClusterConfigurationFactory<java.sql.Driver, DriverDatabase>(DriverDatabaseClusterConfiguration.class, id, properties.getProperty(CONFIG));
		}
		
		return this.factory.createDatabaseCluster(id, factory);
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.util.concurrent.LifecycleRegistry.Factory#getTimeout()
	 */
	@Override
	public long getTimeout()
	{
		return this.timeout;
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.util.concurrent.LifecycleRegistry.Factory#getTimeoutUnit()
	 */
	@Override
	public TimeUnit getTimeoutUnit()
	{
		return this.timeoutUnit;
	}
}
