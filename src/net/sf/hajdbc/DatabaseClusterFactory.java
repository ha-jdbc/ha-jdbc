/*
 * HA-JDBC: High-Availability JDBC
 * Copyright (c) 2004-2006 Paul Ferraro
 * 
 * This library is free software; you can redistribute it and/or modify it 
 * under the terms of the GNU Lesser General Public License as published by the 
 * Free Software Foundation; either version 2.1 of the License, or (at your 
 * option) any later version.
 * 
 * This library is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or 
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public License 
 * for more details.
 * 
 * You should have received a copy of the GNU Lesser General Public License
 * along with this library; if not, write to the Free Software Foundation, 
 * Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA
 * 
 * Contact: ferraro@users.sourceforge.net
 */
package net.sf.hajdbc;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.ResourceBundle;

import javax.management.MBeanServer;
import javax.management.MBeanServerFactory;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.StandardMBean;

import net.sf.hajdbc.local.LocalDatabaseCluster;

import org.jibx.runtime.BindingDirectory;
import org.jibx.runtime.IMarshallingContext;
import org.jibx.runtime.IUnmarshallingContext;
import org.jibx.runtime.JiBXException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author  Paul Ferraro
 * @version $Revision$
 * @since   1.0
 */
public final class DatabaseClusterFactory
{
	private static final String JMX_AGENT_PROPERTY = "ha-jdbc.jmx-agent";
	private static final String CONFIGURATION_PROPERTY = "ha-jdbc.configuration";
	
	private static final String DEFAULT_RESOURCE = "ha-jdbc.xml";
	
	private static final String MBEAN_DOMAIN = "net.sf.hajdbc";
	private static final String MBEAN_CLUSTER_KEY = "cluster";
	private static final String MBEAN_DATABASE_KEY = "database";
	
	private static Logger logger = LoggerFactory.getLogger(DatabaseClusterFactory.class);
	
	private static DatabaseClusterFactory instance = null;
	private static ResourceBundle resource = ResourceBundle.getBundle(DatabaseClusterFactory.class.getName());
		
	/**
	 * Convenience method for constructing a standardized mbean ObjectName for this cluster.
	 * @param databaseClusterId a cluster identifier
	 * @return an ObjectName for this cluster
	 * @throws MalformedObjectNameException if the ObjectName could not be constructed
	 */
	public static ObjectName getObjectName(String databaseClusterId) throws MalformedObjectNameException
	{
		return getObjectName(databaseClusterId, new Properties());
	}

	/**
	 * Convenience method for constructing a standardized mbean ObjectName for this database.
	 * @param databaseClusterId a cluster identifier
	 * @param databaseId a database identifier
	 * @return an ObjectName for this cluster
	 * @throws MalformedObjectNameException if the ObjectName could not be constructed
	 */
	public static ObjectName getObjectName(String databaseClusterId, String databaseId) throws MalformedObjectNameException
	{
		Properties properties = new Properties();
		properties.setProperty(MBEAN_DATABASE_KEY, ObjectName.quote(databaseId));
		
		return getObjectName(databaseClusterId, properties);
	}
	
	private static ObjectName getObjectName(String databaseClusterId, Properties properties) throws MalformedObjectNameException
	{
		properties.setProperty(MBEAN_CLUSTER_KEY, ObjectName.quote(databaseClusterId));
		
		return ObjectName.getInstance(MBEAN_DOMAIN, properties);
	}
	
	/**
	 * Returns the current HA-JDBC version.
	 * @return a version label
	 */
	public static String getVersion()
	{
		return resource.getString("version");
	}
	
	/**
	 * Provides access to the singleton instance of this factory object.
	 * @return a factory for creating database clusters
	 */
	public static synchronized DatabaseClusterFactory getInstance()
	{
		if (instance == null)
		{
			instance = createDatabaseClusterFactory();
		}
		
		return instance;
	}
	
	/**
	 * Returns the mbean server to which the database clusters will be registered.
	 * @return an mbean server instance
	 */
	public static MBeanServer getMBeanServer()
	{
		String agent = System.getProperty(JMX_AGENT_PROPERTY);
		
		List serverList = MBeanServerFactory.findMBeanServer(agent);
		
		if (serverList.isEmpty())
		{
			MBeanServerFactory.createMBeanServer(agent);
			throw new IllegalStateException(Messages.getMessage(Messages.MBEAN_SERVER_NOT_FOUND));
		}
		
		return MBeanServer.class.cast(serverList.get(0));
	}
	
	/**
	 * Creates a new DatabaseClusterFactory from a configuration file.
	 * @return a factory for creating database clusters
	 */
	private static DatabaseClusterFactory createDatabaseClusterFactory()
	{
		String resource = System.getProperty(CONFIGURATION_PROPERTY, DEFAULT_RESOURCE);
		
		URL url = getResourceURL(resource);
		
		logger.info(Messages.getMessage(Messages.HA_JDBC_INIT, getVersion(), url));
		
		InputStream inputStream = null;
		
		try
		{
			inputStream = url.openStream();
			
			IUnmarshallingContext context = BindingDirectory.getFactory(DatabaseClusterFactory.class).createUnmarshallingContext();
			
			DatabaseClusterFactory factory = DatabaseClusterFactory.class.cast(context.unmarshalDocument(new InputStreamReader(inputStream)));

			factory.url = url;
			
			Runtime.getRuntime().addShutdownHook(new Thread(new ShutdownHook(factory)));
			
			return factory;
		}
		catch (IOException e)
		{
			throw new RuntimeException(Messages.getMessage(Messages.CONFIG_NOT_FOUND, url), e);
		}
		catch (JiBXException e)
		{
			throw new RuntimeException(Messages.getMessage(Messages.CONFIG_FAILED, url), e);
		}
		finally
		{
			if (inputStream != null)
			{
				try
				{
					inputStream.close();
				}
				catch (IOException e)
				{
					logger.warn(Messages.getMessage(Messages.STREAM_CLOSE_FAILED, url), e);
				}
			}
		}
	}
	
	/**
	 * Algorithm for searching class loaders for HA-JDBC url.
	 * @param resource a resource name
	 * @return a URL for the HA-JDBC configuration resource
	 */
	private static URL getResourceURL(String resource)
	{
		try
		{
			return new URL(resource);
		}
		catch (MalformedURLException e)
		{
			URL url = Thread.currentThread().getContextClassLoader().getResource(resource);
			
			if (url == null)
			{
				url = DatabaseClusterFactory.class.getClassLoader().getResource(resource);
			}

			if (url == null)
			{
				url = ClassLoader.getSystemResource(resource);
			}
			
			if (url == null)
			{
				throw new RuntimeException(Messages.getMessage(Messages.CONFIG_NOT_FOUND, resource));
			}
			
			return url;
		}
	}
	
	static DatabaseCluster createDatabaseCluster(Object factory)
	{
		DatabaseClusterBuilder builder = DatabaseClusterFactory.class.cast(factory).builder;
		
		return (builder == null) ? new LocalDatabaseCluster() : builder.buildDatabaseCluster();
	}
	
	private Map<String, DatabaseCluster> databaseClusterMap = new HashMap<String, DatabaseCluster>();
	private Map<String, SynchronizationStrategy> synchronizationStrategyMap = new HashMap<String, SynchronizationStrategy>();
	private DatabaseClusterBuilder builder;
	private URL url;
	
	private DatabaseClusterFactory()
	{
		// Do nothing
	}
	
	/**
	 * Returns the database cluster identified by the specified id
	 * @param id a database cluster identifier
	 * @return a database cluster
	 */
	public DatabaseCluster getDatabaseCluster(String id)
	{
		DatabaseCluster databaseCluster = this.databaseClusterMap.get(id);
		
		if (databaseCluster == null)
		{
			throw new IllegalArgumentException(Messages.getMessage(Messages.INVALID_DATABASE_CLUSTER, id));
		}
		
		return databaseCluster;
	}
	
	/**
	 * Returns the synchronization strategy identified by the specified id
	 * @param id a synchronization strategy identifier
	 * @return a synchronization strategy
	 * @throws IllegalArgumentException if the specified identifier is not a valid sychronization strategy
	 */
	public SynchronizationStrategy getSynchronizationStrategy(String id)
	{
		SynchronizationStrategy strategy = this.synchronizationStrategyMap.get(id);
		
		if (strategy == null)
		{
			throw new IllegalArgumentException(Messages.getMessage(Messages.INVALID_SYNC_STRATEGY, id));
		}
		
		return strategy;
	}
	
	/**
	 * Exports the current HA-JDBC configuration.
	 */
	public synchronized void export()
	{
		OutputStream outputStream = null;
		
		try
		{
			// We cannot use URLConnection for files becuase Sun's implementation does not support output.
			if (this.url.getProtocol().equals("file"))
			{
				outputStream = new FileOutputStream(new File(this.url.getPath()));
			}
			else
			{
				URLConnection connection = this.url.openConnection();
				
				connection.connect();
				
				outputStream = connection.getOutputStream();
			}
			
			IMarshallingContext context = BindingDirectory.getFactory(DatabaseClusterFactory.class).createMarshallingContext();

			context.setIndent(1, System.getProperty("line.separator"), '\t');
			
			context.marshalDocument(this, null, null, outputStream);
		}
		catch (IOException e)
		{
			logger.warn(e.getMessage(), e);
			throw new RuntimeException(Messages.getMessage(Messages.CONFIG_NOT_FOUND, this.url), e);
		}
		catch (JiBXException e)
		{
			logger.warn(e.getMessage(), e);
			throw new RuntimeException(Messages.getMessage(Messages.CONFIG_FAILED, this.url), e);
		}
		finally
		{
			if (outputStream != null)
			{
				try
				{
					outputStream.close();
				}
				catch (IOException e)
				{
					logger.warn(Messages.getMessage(Messages.STREAM_CLOSE_FAILED, this.url), e);
				}
			}
		}
	}
	
	void addDatabaseCluster(DatabaseCluster databaseCluster) throws Exception
	{
		try
		{
			databaseCluster.start();
			
			ObjectName name = getObjectName(databaseCluster.getId());
			
			MBeanServer server = getMBeanServer();
			
			if (!server.isRegistered(name))
			{
				server.registerMBean(new StandardMBean(databaseCluster, DatabaseClusterMBean.class), name);
			}
			
			this.databaseClusterMap.put(databaseCluster.getId(), databaseCluster);
		}
		catch (Exception e)
		{
			// Log exception here, since it will be masked by JiBX
			logger.error(e.getMessage(), e);
			throw e;
		}
	}
	
	void addSynchronizationStrategyBuilder(SynchronizationStrategyBuilder builder) throws Exception
	{
		this.synchronizationStrategyMap.put(builder.getId(), builder.buildStrategy());
	}
	
	Iterator<SynchronizationStrategyBuilder> getSynchronizationStrategyBuilders() throws Exception
	{
		List<SynchronizationStrategyBuilder> builderList = new ArrayList<SynchronizationStrategyBuilder>(this.synchronizationStrategyMap.size());
		
		for (Map.Entry<String, SynchronizationStrategy> mapEntry: this.synchronizationStrategyMap.entrySet())
		{
			builderList.add(SynchronizationStrategyBuilder.getBuilder(mapEntry.getKey(), mapEntry.getValue()));
		}
		
		return builderList.iterator();
	}
	
	Iterator<DatabaseCluster> getDatabaseClusters()
	{
		return this.databaseClusterMap.values().iterator();
	}
	
	private static class ShutdownHook implements Runnable
	{
		private DatabaseClusterFactory factory;
		
		/**
		 * Constructs a new ShutdownHook.
		 * @param factory
		 */
		public ShutdownHook(DatabaseClusterFactory factory)
		{
			this.factory = factory;
		}
		
		/**
		 * @see java.lang.Runnable#run()
		 */
		public void run()
		{
			Iterator<DatabaseCluster> databaseClusters = this.factory.getDatabaseClusters();
			
			while (databaseClusters.hasNext())
			{
				databaseClusters.next().stop();
			}
		}	
	}
}
