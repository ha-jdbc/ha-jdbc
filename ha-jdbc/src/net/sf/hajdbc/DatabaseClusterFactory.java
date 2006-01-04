/*
 * HA-JDBC: High-Availability JDBC
 * Copyright (C) 2004 Paul Ferraro
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

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.management.MBeanServer;
import javax.management.MBeanServerFactory;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.StandardMBean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jibx.runtime.BindingDirectory;
import org.jibx.runtime.IBindingFactory;
import org.jibx.runtime.IUnmarshallingContext;
import org.jibx.runtime.JiBXException;

/**
 * @author  Paul Ferraro
 * @version $Revision$
 * @since   1.0
 */
public final class DatabaseClusterFactory
{
	private static final String SYSTEM_PROPERTY = "ha-jdbc.configuration";
	private static final String DEFAULT_RESOURCE = "ha-jdbc.xml";
	
	private static final String MBEAN_DOMAIN = "net.sf.hajdbc";
	private static final String MBEAN_KEY = "database-cluster";
	
	private static Log log = LogFactory.getLog(DatabaseClusterFactory.class);
	
	private static DatabaseClusterFactory instance = null;
	
	/**
	 * Convenience method for constructing an mbean ObjectName for this cluster.
	 * The ObjectName is constructed using {@link #MBEAN_DOMAIN} and {@link #MBEAN_KEY} and the quoted cluster identifier.
	 * @param databaseClusterId a cluster identifier
	 * @return an ObjectName for this cluster
	 * @throws MalformedObjectNameException if the ObjectName could not be constructed
	 */
	public static ObjectName getObjectName(String databaseClusterId) throws MalformedObjectNameException
	{
		return ObjectName.getInstance(MBEAN_DOMAIN, MBEAN_KEY, ObjectName.quote(databaseClusterId));
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
	 * Creates a new DatabaseClusterFactory from a configuration file.
	 * @return a factory for creating database clusters
	 */
	private static DatabaseClusterFactory createDatabaseClusterFactory()
	{
		String resourceName = System.getProperty(SYSTEM_PROPERTY, DEFAULT_RESOURCE);
		
		URL resourceURL = getResourceURL(resourceName);
		
		if (resourceURL == null)
		{
			throw new RuntimeException(Messages.getMessage(Messages.CONFIG_NOT_FOUND, resourceName));
		}
		
		InputStream inputStream = null;
		
		try
		{
			inputStream = resourceURL.openStream();
			
			IBindingFactory factory = BindingDirectory.getFactory(DatabaseClusterFactory.class);
			IUnmarshallingContext context = factory.createUnmarshallingContext();
			
			return (DatabaseClusterFactory) context.unmarshalDocument(new InputStreamReader(inputStream));
		}
		catch (IOException e)
		{
			throw new RuntimeException(Messages.getMessage(Messages.CONFIG_NOT_FOUND, resourceURL), e);
		}
		catch (JiBXException e)
		{
			throw new RuntimeException(Messages.getMessage(Messages.CONFIG_FAILED, resourceURL), e);
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
					log.warn(Messages.getMessage(Messages.STREAM_CLOSE_FAILED, resourceURL), e);
				}
			}
		}
	}
	
	/**
	 * Algorithm for searching class loaders for a specified resource.
	 * @param resourceName a resource to find
	 * @return a URL for the resource
	 */
	private static URL getResourceURL(String resourceName)
	{
		try
		{
			return new URL(resourceName);
		}
		catch (MalformedURLException e)
		{
			URL url = Thread.currentThread().getContextClassLoader().getResource(resourceName);
			
			if (url == null)
			{
				url = DatabaseClusterFactory.class.getClassLoader().getResource(resourceName);
			}

			if (url == null)
			{
				url = ClassLoader.getSystemResource(resourceName);
			}
			
			return url;
		}
	}
	
	private Map databaseClusterMap = new HashMap();
	private Map synchronizationStrategyMap = new HashMap();
	private DatabaseClusterDecorator decorator;
	private MBeanServer server;
	
	private DatabaseClusterFactory()
	{
		List serverList = MBeanServerFactory.findMBeanServer(null);
		
		if (serverList.isEmpty())
		{
			throw new IllegalStateException(Messages.getMessage(Messages.MBEAN_SERVER_NOT_FOUND));
		}
		
		this.server = (MBeanServer) serverList.get(0);
	}
	
	/**
	 * Returns the database cluster identified by the specified id
	 * @param id a database cluster identifier
	 * @return a database cluster
	 */
	public DatabaseCluster getDatabaseCluster(String id)
	{
		return (DatabaseCluster) this.databaseClusterMap.get(id);
	}
	
	/**
	 * Returns the synchronization strategy identified by the specified id
	 * @param id a synchronization strategy identifier
	 * @return a synchronization strategy
	 * @throws java.sql.SQLException if the specified identifier is not a valid sychronization strategy
	 */
	public SynchronizationStrategy getSynchronizationStrategy(String id) throws java.sql.SQLException
	{
		SynchronizationStrategy strategy = (SynchronizationStrategy) this.synchronizationStrategyMap.get(id);
		
		if (strategy == null)
		{
			throw new SQLException(Messages.getMessage(Messages.INVALID_SYNC_STRATEGY, id));
		}
		
		return strategy;
	}
	
	void addDatabaseCluster(DatabaseCluster databaseCluster) throws Exception
	{
		if (this.decorator != null)
		{
			databaseCluster = this.decorator.decorate(databaseCluster);
		}
		
		databaseCluster.init();

		ObjectName name = getObjectName(databaseCluster.getId());
		
		if (!this.server.isRegistered(name))
		{
			this.server.registerMBean(new StandardMBean(databaseCluster, DatabaseClusterMBean.class), name);
		}
		
		this.databaseClusterMap.put(databaseCluster.getId(), databaseCluster);
	}
	
	void addSynchronizationStrategy(SynchronizationStrategy strategy) throws Exception
	{
		this.synchronizationStrategyMap.put(strategy.getId(), strategy);
	}
}
