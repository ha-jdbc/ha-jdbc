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
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.SortedMap;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import net.sf.hajdbc.AbstractDriver;
import net.sf.hajdbc.DatabaseCluster;
import net.sf.hajdbc.DatabaseClusterConfiguration;
import net.sf.hajdbc.DatabaseClusterConfigurationFactory;
import net.sf.hajdbc.ExceptionType;
import net.sf.hajdbc.Messages;
import net.sf.hajdbc.logging.Level;
import net.sf.hajdbc.logging.Logger;
import net.sf.hajdbc.logging.LoggerFactory;
import net.sf.hajdbc.util.concurrent.MapRegistryStoreFactory;
import net.sf.hajdbc.util.concurrent.Registry;
import net.sf.hajdbc.util.reflect.ProxyFactory;
import net.sf.hajdbc.xml.XMLDatabaseClusterConfigurationFactory;

/**
 * @author  Paul Ferraro
 */
public final class Driver extends AbstractDriver implements Registry.Factory<String, DatabaseCluster<java.sql.Driver, DriverDatabase>, Properties, SQLException>
{
	private static final Pattern URL_PATTERN = Pattern.compile("jdbc:ha-jdbc:(.+)"); //$NON-NLS-1$
	private static final String CONFIG = "config"; //$NON-NLS-1$
	
	private static final Logger logger = LoggerFactory.getLogger(Driver.class);
	
	private static volatile Map<String, DatabaseClusterConfigurationFactory<java.sql.Driver, DriverDatabase>> configurationFactoryMap = Collections.emptyMap();
	private static volatile long timeout = 10;
	private static volatile TimeUnit timeoutUnit = TimeUnit.SECONDS;
	
	private static final Registry<String, DatabaseCluster<java.sql.Driver, DriverDatabase>, Properties, SQLException> registry = new Registry<String, DatabaseCluster<java.sql.Driver, DriverDatabase>, Properties, SQLException>(new Driver(), new MapRegistryStoreFactory<String>(), ExceptionType.getExceptionFactory(SQLException.class));
	
	static
	{
		try
		{
			DriverManager.registerDriver(new Driver());
		}
		catch (SQLException e)
		{
			logger.log(Level.ERROR, e, Messages.DRIVER_REGISTER_FAILED.getMessage(), Driver.class.getName());
		}
	}

	/**
	 * Set custom configuration factories per cluster.
	 * @param factories a map of configuration factories per cluster identifier.
	 */
	public static void setConfigurationFactories(Map<String, DatabaseClusterConfigurationFactory<java.sql.Driver, DriverDatabase>> factories)
	{
		configurationFactoryMap = (factories != null) ? factories : Collections.<String, DatabaseClusterConfigurationFactory<java.sql.Driver, DriverDatabase>>emptyMap();
	}
	
	public static void setTimeout(long value, TimeUnit unit)
	{
		timeout = value;
		timeoutUnit = unit;
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
		
		DatabaseCluster<java.sql.Driver, DriverDatabase> cluster = registry.get(id, properties);
		
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
		
		DatabaseCluster<java.sql.Driver, DriverDatabase> cluster = registry.get(id, properties);
		
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
	 * @see net.sf.hajdbc.util.concurrent.Registry.Factory#create(java.lang.Object, java.lang.Object)
	 */
	@Override
	public DatabaseCluster<java.sql.Driver, DriverDatabase> create(String id, Properties properties) throws SQLException
	{
		DatabaseClusterConfigurationFactory<java.sql.Driver, DriverDatabase> factory = configurationFactoryMap.get(id);
		
		if (factory == null)
		{
			factory = new XMLDatabaseClusterConfigurationFactory<java.sql.Driver, DriverDatabase>(DriverDatabaseClusterConfiguration.class, id, properties.getProperty(CONFIG));
		}
		
		DatabaseClusterConfiguration<java.sql.Driver, DriverDatabase> configuration = factory.createConfiguration();
		
		return new DatabaseClusterImpl<java.sql.Driver, DriverDatabase>(id, configuration, factory);
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.util.concurrent.Registry.Factory#getTimeout()
	 */
	@Override
	public long getTimeout()
	{
		return timeout;
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.util.concurrent.Registry.Factory#getTimeoutUnit()
	 */
	@Override
	public TimeUnit getTimeoutUnit()
	{
		return timeoutUnit;
	}
}
