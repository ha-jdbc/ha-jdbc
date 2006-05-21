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
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.ResourceBundle;

import javax.management.InstanceAlreadyExistsException;
import javax.management.MBeanRegistration;
import javax.management.MBeanRegistrationException;
import javax.management.MBeanServer;
import javax.management.MBeanServerFactory;
import javax.management.MBeanServerInvocationHandler;
import javax.management.MalformedObjectNameException;
import javax.management.NotCompliantMBeanException;
import javax.management.ObjectName;
import javax.management.StandardMBean;

import net.sf.hajdbc.local.LocalDatabaseCluster;
import net.sf.hajdbc.util.LinkedHashtable;

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
public final class DatabaseClusterFactory implements MBeanRegistration, DatabaseClusterFactoryMBean
{
	private static final String JMX_AGENT_PROPERTY = "ha-jdbc.jmx-agent";
	
	private static final String RESOURCE = System.getProperty("ha-jdbc.configuration", "ha-jdbc.xml");
	
	private static final String MBEAN_DOMAIN = "net.sf.hajdbc";
	private static final String MBEAN_TYPE_KEY = "type";
	private static final String MBEAN_CLUSTER_KEY = "cluster";
	private static final String MBEAN_DATABASE_KEY = "database";
	private static final String MBEAN_CLUSTER_TYPE = "Clusters";
	private static final String MBEAN_FACTORY_TYPE = "Factory";
	
	static Logger logger = LoggerFactory.getLogger(DatabaseClusterFactory.class);
	
	private static ResourceBundle resource = ResourceBundle.getBundle(DatabaseClusterFactory.class.getName());

	/**
	 * Convenience method for constructing a standardized mbean ObjectName for the database cluster factory.
	 * @return an ObjectName for this cluster factory
	 * @throws MalformedObjectNameException if the ObjectName could not be constructed
	 */
	public static ObjectName getObjectName()
	{
		Hashtable<String, String> properties = new Hashtable<String, String>();
		
		properties.put(MBEAN_TYPE_KEY, MBEAN_FACTORY_TYPE);
		
		return getObjectName(properties);
	}
	
	/**
	 * Convenience method for constructing a standardized mbean ObjectName for the specified cluster.
	 * @param databaseClusterId a cluster identifier
	 * @return an ObjectName for this cluster
	 * @throws MalformedObjectNameException if the ObjectName could not be constructed
	 */
	public static ObjectName getObjectName(String databaseClusterId)
	{
		return getObjectName(createClusterProperties(databaseClusterId));
	}

	/**
	 * Convenience method for constructing a standardized mbean ObjectName for the specified database.
	 * @param databaseClusterId a cluster identifier
	 * @param databaseId a database identifier
	 * @return an ObjectName for this cluster
	 * @throws MalformedObjectNameException if the ObjectName could not be constructed
	 */
	public static ObjectName getObjectName(String databaseClusterId, String databaseId)
	{
		Hashtable<String, String> properties = createClusterProperties(databaseClusterId);
		
		properties.put(MBEAN_DATABASE_KEY, ObjectName.quote(databaseId));
		
		return getObjectName(properties);
	}

	private static ObjectName getObjectName(Hashtable<String, String> properties)
	{
		try
		{
			return ObjectName.getInstance(MBEAN_DOMAIN, properties);
		}
		catch (MalformedObjectNameException e)
		{
			throw new IllegalArgumentException(properties.toString());
		}
	}
	
	private static Hashtable<String, String> createClusterProperties(String databaseClusterId)
	{
		Hashtable<String, String> properties = new LinkedHashtable<String, String>();
		
		properties.put(MBEAN_TYPE_KEY, MBEAN_CLUSTER_TYPE);
		properties.put(MBEAN_CLUSTER_KEY, ObjectName.quote(databaseClusterId));
		
		return properties;
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
	public static synchronized DatabaseClusterFactoryMBean getInstance()
	{
		try
		{
			ObjectName name = getObjectName();
			MBeanServer server = getMBeanServer();
			
			if (server.isRegistered(name))
			{
				return (DatabaseClusterFactoryMBean) MBeanServerInvocationHandler.newProxyInstance(server, name, DatabaseClusterFactoryMBean.class, false);
			}
			
			DatabaseClusterFactory factory = createDatabaseClusterFactory();
			
			server.registerMBean(new StandardMBean(factory, DatabaseClusterFactoryMBean.class), name);
			
			return factory;
		}
		catch (InstanceAlreadyExistsException e)
		{
			throw new IllegalStateException(e);
		}
		catch (MBeanRegistrationException e)
		{
			throw new IllegalStateException(e);
		}
		catch (NotCompliantMBeanException e)
		{
			throw new IllegalStateException(e);
		}
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
		URL url = getResourceURL();
		
		logger.info(Messages.getMessage(Messages.HA_JDBC_INIT, getVersion(), url));
		
		InputStream inputStream = null;
		
		try
		{
			inputStream = url.openStream();
			
			IUnmarshallingContext context = BindingDirectory.getFactory(DatabaseClusterFactory.class).createUnmarshallingContext();
			
			return DatabaseClusterFactory.class.cast(context.unmarshalDocument(inputStream, null));
		}
		catch (IOException e)
		{
			String message = Messages.getMessage(Messages.CONFIG_NOT_FOUND, url);
			
			logger.error(message, e);
			
			throw new RuntimeException(message, e);
		}
		catch (JiBXException e)
		{
			String message = Messages.getMessage(Messages.CONFIG_LOAD_FAILED, url);
			
			logger.error(message, e);
			
			throw new RuntimeException(message, e);
		}
		catch (RuntimeException e)
		{
			logger.error(e.getMessage(), e);
			
			throw e;
		}
		catch (Error e)
		{
			logger.error(e.getMessage(), e);
			
			throw e;
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
					logger.warn(e.toString(), e);
				}
			}
		}
	}
	
	/**
	 * Algorithm for searching class loaders for HA-JDBC url.
	 * @param resource a resource name
	 * @return a URL for the HA-JDBC configuration resource
	 */
	private static URL getResourceURL()
	{
		try
		{
			return new URL(RESOURCE);
		}
		catch (MalformedURLException e)
		{
			URL url = Thread.currentThread().getContextClassLoader().getResource(RESOURCE);
			
			if (url == null)
			{
				url = DatabaseClusterFactory.class.getClassLoader().getResource(RESOURCE);
			}

			if (url == null)
			{
				url = ClassLoader.getSystemResource(RESOURCE);
			}
			
			if (url == null)
			{
				throw new IllegalArgumentException(Messages.getMessage(Messages.CONFIG_NOT_FOUND, RESOURCE));
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
	public synchronized void exportConfiguration()
	{
		File file = null;
		WritableByteChannel outputChannel = null;
		FileChannel fileChannel = null;
		
		URL url = getResourceURL();
		
		try
		{
			file = this.exportToFile();
			
			fileChannel = new FileInputStream(file).getChannel();

			// We cannot use URLConnection for files becuase Sun's implementation does not support output.
			if (url.getProtocol().equals("file"))
			{
				outputChannel = new FileOutputStream(new File(url.getPath())).getChannel();
			}
			else
			{
				URLConnection connection = url.openConnection();
				
				connection.connect();
				
				outputChannel = Channels.newChannel(connection.getOutputStream());
			}
			
			fileChannel.transferTo(0, file.length(), outputChannel);
		}
		catch (Exception e)
		{
			logger.warn(Messages.getMessage(Messages.CONFIG_STORE_FAILED, url), e);
		}
		finally
		{
			if (outputChannel != null)
			{
				try
				{
					outputChannel.close();
				}
				catch (IOException e)
				{
					logger.warn(e.toString(), e);
				}
			}
			
			if (fileChannel != null)
			{
				try
				{
					fileChannel.close();
				}
				catch (IOException e)
				{
					logger.warn(e.toString(), e);
				}
			}
			
			if (file != null)
			{
				file.delete();
			}
		}
	}
	
	private File exportToFile() throws Exception
	{
		File file = File.createTempFile("ha-jdbc", ".xml");
		
		IMarshallingContext context = BindingDirectory.getFactory(DatabaseClusterFactory.class).createMarshallingContext();
	
		context.setIndent(1, System.getProperty("line.separator"), '\t');
		
		// This method closes the writer
		context.marshalDocument(this, null, null, new FileWriter(file));
		
		return file;
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
			logger.error(e.toString(), e);
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

	/**
	 * @see javax.management.MBeanRegistration#preRegister(javax.management.MBeanServer, javax.management.ObjectName)
	 */
	public ObjectName preRegister(MBeanServer server, ObjectName name) throws Exception
	{
		return name;
	}

	/**
	 * @see javax.management.MBeanRegistration#postRegister(java.lang.Boolean)
	 */
	public void postRegister(Boolean registrationDone)
	{
		// Do nothing
	}

	/**
	 * @see javax.management.MBeanRegistration#preDeregister()
	 */
	public void preDeregister() throws Exception
	{
		logger.info(Messages.getMessage(Messages.SHUT_DOWN));
		
		Iterator<DatabaseCluster> databaseClusters = this.getDatabaseClusters();
		
		while (databaseClusters.hasNext())
		{
			databaseClusters.next().stop();
		}
	}

	/**
	 * @see javax.management.MBeanRegistration#postDeregister()
	 */
	public void postDeregister()
	{
		// Do nothing
	}
}
