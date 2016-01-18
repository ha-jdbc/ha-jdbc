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
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.time.Duration;
import java.util.AbstractMap;
import java.util.Map;
import java.util.Properties;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;
import java.util.regex.Pattern;

import net.sf.hajdbc.DatabaseCluster;
import net.sf.hajdbc.DatabaseClusterConfigurationFactory;
import net.sf.hajdbc.DatabaseClusterFactory;
import net.sf.hajdbc.invocation.InvocationStrategies;
import net.sf.hajdbc.invocation.Invoker;
import net.sf.hajdbc.logging.Level;
import net.sf.hajdbc.logging.Logger;
import net.sf.hajdbc.logging.LoggerFactory;
import net.sf.hajdbc.messages.Messages;
import net.sf.hajdbc.messages.MessagesFactory;
import net.sf.hajdbc.xml.XMLDatabaseClusterConfigurationFactory;

/**
 * @author  Paul Ferraro
 */
public class Driver extends AbstractDriver
{
	private static final Pattern URL_PATTERN = Pattern.compile("jdbc:ha-jdbc:(?://)?([^/]+)(?:/.+)?");
	private static final String CONFIG = "config";

	private static final Messages messages = MessagesFactory.getMessages();
	static final Logger logger = LoggerFactory.getLogger(Driver.class);

	static volatile Duration timeout = Duration.ofSeconds(10);
	static volatile DatabaseClusterFactory<java.sql.Driver, DriverDatabase> factory = new DatabaseClusterFactoryImpl<>();
	
	static final Map<String, DatabaseClusterConfigurationFactory<java.sql.Driver, DriverDatabase>> configurationFactories = new ConcurrentHashMap<>();
	static final ConcurrentMap<String, Map.Entry<DriverProxyFactory, java.sql.Driver>> proxies = new ConcurrentHashMap<>();

	static
	{
		try
		{
			Driver driver = new Driver()
			{
				@Override
				protected void finalize()
				{
					// When the driver instance that was registered with the DriverManager is finalized, close any clusters
					for (String id : proxies.keySet())
					{
						try
						{
							close(id);
						}
						catch (Throwable e)
						{
							e.printStackTrace(DriverManager.getLogWriter());
						}
					}
				}
			};
			DriverManager.registerDriver(driver);
		}
		catch (SQLException e)
		{
			logger.log(Level.ERROR, messages.registerDriverFailed(Driver.class), e);
		}
	}
	
	public static void close(String id)
	{
		Map.Entry<DriverProxyFactory, java.sql.Driver> entry = proxies.remove(id);
		if (entry != null)
		{
			DriverProxyFactory factory = entry.getKey();
			factory.close();
			factory.getDatabaseCluster().stop();
		}
	}

	public static void setFactory(DatabaseClusterFactory<java.sql.Driver, DriverDatabase> factory)
	{
		Driver.factory = factory;
	}

	public static void setConfigurationFactory(String id, DatabaseClusterConfigurationFactory<java.sql.Driver, DriverDatabase> configurationFactory)
	{
		configurationFactories.put(id,  configurationFactory);
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.sql.AbstractDriver#getUrlPattern()
	 */
	@Override
	protected Pattern getUrlPattern()
	{
		return URL_PATTERN;
	}

	private static Map.Entry<DriverProxyFactory, java.sql.Driver> getProxyEntry(String id, Properties properties)
	{
		Function<String, Map.Entry<DriverProxyFactory, java.sql.Driver>> function = (String key) ->
		{
			DatabaseClusterConfigurationFactory<java.sql.Driver, DriverDatabase> configurationFactory = configurationFactories.get(key);
			
			if (configurationFactory == null)
			{
				String config = (properties != null) ? properties.getProperty(CONFIG) : null;
				configurationFactory = new XMLDatabaseClusterConfigurationFactory<>(id, config);
			}
			try
			{
				DatabaseCluster<java.sql.Driver, DriverDatabase> cluster = factory.createDatabaseCluster(key, configurationFactory, new DriverDatabaseClusterConfigurationBuilder());
				cluster.start();
				DriverProxyFactory factory = new DriverProxyFactory(cluster);
				return new AbstractMap.SimpleImmutableEntry<>(factory, factory.createProxy());
			}
			catch (SQLException e)
			{
				throw new IllegalStateException(e);
			}
		};
		return proxies.computeIfAbsent(id, function);
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
		
		Map.Entry<DriverProxyFactory, java.sql.Driver> entry = getProxyEntry(id, properties);
		TransactionContext<java.sql.Driver, DriverDatabase> context = new LocalTransactionContext<>(entry.getKey().getDatabaseCluster());

		ConnectionProxyFactoryFactory<java.sql.Driver, DriverDatabase, java.sql.Driver> factory = new ConnectionProxyFactoryFactory<>(context);
		DriverInvoker<Connection> invoker = (DriverDatabase database, java.sql.Driver driver) -> driver.connect(database.getLocation(), properties);
		return factory.createProxyFactory(entry.getValue(), entry.getKey(), invoker, InvocationStrategies.INVOKE_ON_ALL.invoke(entry.getKey(), invoker)).createProxy();
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
		
		Map.Entry<DriverProxyFactory, java.sql.Driver> entry = getProxyEntry(id, properties);
		DriverInvoker<DriverPropertyInfo[]> invoker = (DriverDatabase database, java.sql.Driver driver) -> driver.getPropertyInfo(database.getLocation(), properties);
		SortedMap<DriverDatabase, DriverPropertyInfo[]> results = InvocationStrategies.INVOKE_ON_ANY.invoke(entry.getKey(), invoker);
		return results.get(results.firstKey());
	}

	/**
	 * @see java.sql.Driver#getParentLogger()
	 */
	@Override
	public java.util.logging.Logger getParentLogger()
	{
		return java.util.logging.Logger.getGlobal();
	}

	private interface DriverInvoker<R> extends Invoker<java.sql.Driver, DriverDatabase, java.sql.Driver, R, SQLException>
	{
	}
}
