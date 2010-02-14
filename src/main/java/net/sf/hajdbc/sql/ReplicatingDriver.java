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
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Pattern;

import net.sf.hajdbc.AbstractDriver;
import net.sf.hajdbc.DatabaseCluster;
import net.sf.hajdbc.DatabaseClusterConfiguration;
import net.sf.hajdbc.DatabaseClusterConfigurationFactory;
import net.sf.hajdbc.Messages;
import net.sf.hajdbc.logging.Level;
import net.sf.hajdbc.logging.Logger;
import net.sf.hajdbc.logging.LoggerFactory;
import net.sf.hajdbc.util.reflect.ProxyFactory;
import net.sf.hajdbc.xml.XMLDatabaseClusterConfigurationFactory;

/**
 * @author  Paul Ferraro
 */
public final class ReplicatingDriver extends AbstractDriver
{
	private static final Pattern URL_PATTERN = Pattern.compile("jdbc:ha-jdbc:(.+)"); //$NON-NLS-1$
	private static final String CONFIG = "config"; //$NON-NLS-1$
	
	private static final Logger logger = LoggerFactory.getLogger(ReplicatingDriver.class);
	
	private static final Map<String, DatabaseCluster<java.sql.Driver, DriverDatabase>> clusterMap = new HashMap<String, DatabaseCluster<java.sql.Driver, DriverDatabase>>();

	static
	{
		try
		{
			DriverManager.registerDriver(new ReplicatingDriver());
		}
		catch (SQLException e)
		{
			logger.log(Level.ERROR, e, Messages.DRIVER_REGISTER_FAILED.getMessage(), ReplicatingDriver.class.getName());
		}
	}
	
	private DatabaseClusterConfigurationFactory<Driver, DriverDatabase> configurationFactory;

	public void setConfigurationFactory(DatabaseClusterConfigurationFactory<Driver, DriverDatabase> configurationFactory)
	{
		this.configurationFactory = configurationFactory;
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
	 * @see java.sql.Driver#connect(java.lang.String, java.util.Properties)
	 */
	@Override
	public Connection connect(String url, final Properties properties) throws SQLException
	{
		String id = this.parse(url);
		
		if (id == null) return null;
		
		DatabaseCluster<Driver, DriverDatabase> cluster = this.getDatabaseCluster(id, properties);
		
		DriverInvocationHandler handler = new DriverInvocationHandler(cluster);
		
		java.sql.Driver driver = ProxyFactory.createProxy(java.sql.Driver.class, handler);
		
		Invoker<Driver, DriverDatabase, Driver, Connection, SQLException> invoker = new Invoker<Driver, DriverDatabase, Driver, Connection, SQLException>()
		{
			public Connection invoke(DriverDatabase database, Driver driver) throws SQLException
			{
				return driver.connect(database.getName(), properties);
			}
		};
		
		TransactionContext<Driver, DriverDatabase> context = new LocalTransactionContext<Driver, DriverDatabase>(cluster);
		
		return new ConnectionInvocationStrategy<Driver, DriverDatabase, Driver>(cluster, driver, context).invoke(handler, invoker);
	}
	
	/**
	 * @see Driver#getPropertyInfo(java.lang.String, java.util.Properties)
	 */
	@Override
	public DriverPropertyInfo[] getPropertyInfo(String url, final Properties properties) throws SQLException
	{
		String id = this.parse(url);
		
		if (id == null) return null;
		
		DatabaseCluster<Driver, DriverDatabase> cluster = this.getDatabaseCluster(id, properties);
		
		DriverInvocationHandler handler = new DriverInvocationHandler(cluster);
		
		Invoker<Driver, DriverDatabase, Driver, DriverPropertyInfo[], SQLException> invoker = new Invoker<Driver, DriverDatabase, Driver, DriverPropertyInfo[], SQLException>()
		{
			public DriverPropertyInfo[] invoke(DriverDatabase database, Driver driver) throws SQLException
			{
				return driver.getPropertyInfo(database.getName(), properties);
			}			
		};
		
		return new DatabaseReadInvocationStrategy<Driver, DriverDatabase, Driver, DriverPropertyInfo[], SQLException>().invoke(handler, invoker);
	}

	private DatabaseCluster<Driver, DriverDatabase> getDatabaseCluster(String id, Properties properties) throws SQLException
	{
		synchronized (clusterMap)
		{
			DatabaseCluster<Driver, DriverDatabase> cluster = clusterMap.get(id);
			
			if (cluster == null)
			{
				DatabaseClusterConfigurationFactory<Driver, DriverDatabase> factory = (this.configurationFactory != null) ? this.configurationFactory : new XMLDatabaseClusterConfigurationFactory<Driver, DriverDatabase>(id, properties.getProperty(CONFIG));
				
				DatabaseClusterConfiguration<Driver, DriverDatabase> configuration = factory.createConfiguration(DriverDatabaseClusterConfiguration.class);
				
				cluster = new DatabaseClusterImpl<Driver, DriverDatabase>(id, configuration, factory);
				
				try
				{
					cluster.start();
					
					clusterMap.put(id, cluster);
				}
				catch (Exception e)
				{
					cluster.stop();

					throw SQLExceptionFactory.getInstance().createException(e);
				}
			}
			
			return cluster;
		}
	}
}
