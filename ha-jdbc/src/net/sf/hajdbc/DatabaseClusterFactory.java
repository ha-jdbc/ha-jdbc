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
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import javax.management.MBeanServer;
import javax.management.MBeanServerFactory;
import javax.management.ObjectName;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jibx.runtime.BindingDirectory;
import org.jibx.runtime.IBindingFactory;
import org.jibx.runtime.IUnmarshallingContext;

/**
 * @author  Paul Ferraro
 * @version $Revision$
 * @since   1.0
 */
public final class DatabaseClusterFactory
{
	private static final String SYSTEM_PROPERTY = "ha-jdbc.configuration";
	private static final String DEFAULT_RESOURCE = "ha-jdbc.xml";
	
	private static Log log = LogFactory.getLog(DatabaseClusterFactory.class);
	
	private static DatabaseClusterFactory instance = null;
	
	/**
	 * Provides access to the singleton instance of this factory object.
	 * @return a factory for creating database clusters
	 * @throws java.sql.SQLException if factory instantiation fails
	 */
	public static synchronized DatabaseClusterFactory getInstance() throws java.sql.SQLException
	{
		if (instance == null)
		{
			instance = new DatabaseClusterFactory();
		}
		
		return instance;
	}
	
	private Map databaseClusterMap;
	
	/**
	 * Constructs a new DatabaseClusterFactory.
	 * @throws java.sql.SQLException if construction fails
	 */
	private DatabaseClusterFactory() throws java.sql.SQLException
	{
		String resourceName = System.getProperty(SYSTEM_PROPERTY, DEFAULT_RESOURCE);
		
		URL resourceURL = getResourceURL(resourceName);
		
		if (resourceURL == null)
		{
			throw new SQLException(Messages.getMessage(Messages.CONFIG_NOT_FOUND, resourceName));
		}
		
		InputStream inputStream = null;
		MBeanServer server = null;
		
		try
		{
			inputStream = resourceURL.openStream();
			
			IBindingFactory factory = BindingDirectory.getFactory(Configuration.class);
			IUnmarshallingContext context = factory.createUnmarshallingContext();
			
			Configuration configuration = (Configuration) context.unmarshalDocument(new InputStreamReader(inputStream));

			inputStream.close();
			inputStream = null;
			
			List serverList = MBeanServerFactory.findMBeanServer(null);
			
			if (serverList.isEmpty())
			{
				server = MBeanServerFactory.createMBeanServer();
			}
			else
			{
				server = (MBeanServer) serverList.get(0);
			}
			
			List descriptorList = configuration.getDescriptorList();
			this.databaseClusterMap = new HashMap(descriptorList.size());
			Iterator descriptors = descriptorList.iterator();
			
			while (descriptors.hasNext())
			{
				DatabaseClusterDescriptor descriptor = (DatabaseClusterDescriptor) descriptors.next();
				
				DatabaseCluster databaseCluster = descriptor.createDatabaseCluster();
				DatabaseClusterDecoratorDescriptor decoratorDescriptor = configuration.getDecoratorDescriptor();
				
				if (decoratorDescriptor != null)
				{
					databaseCluster = decoratorDescriptor.decorate(databaseCluster);
				}
				
				databaseCluster.init();
				
				ObjectName name = DatabaseCluster.getObjectName(databaseCluster.getId());
				
				if (!server.isRegistered(name))
				{
					server.registerMBean(databaseCluster, name);
				}
				
				this.databaseClusterMap.put(databaseCluster.getId(), databaseCluster);
			}
		}
		catch (Exception e)
		{
			String message = Messages.getMessage(Messages.CONFIG_FAILED, resourceURL);
			
			log.warn(message, e);
			
			throw new SQLException(message, e);
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
	 * Returns the database cluster identified by the specified id
	 * @param id a database cluster identifier
	 * @return a database cluster
	 */
	public DatabaseCluster getDatabaseCluster(String id)
	{
		return (DatabaseCluster) this.databaseClusterMap.get(id);
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
	
	/**
	 * Base xml binding object used by JiBX
	 */
	private static class Configuration
	{
		private DatabaseClusterDecoratorDescriptor decoratorDescriptor = null;
		private List descriptorList = new LinkedList();
		
		/**
		 * Returns a descriptor of a database cluster decorator.
		 * @return a DatabaseClusterDecoratorDescriptor, or null if one was not defined
		 */
		public DatabaseClusterDecoratorDescriptor getDecoratorDescriptor()
		{
			return this.decoratorDescriptor;
		}
		
		/**
		 * Returns a list of database cluster descriptors
		 * @return a List<DatabaseClusterDescriptor>
		 */
		public List getDescriptorList()
		{
			return this.descriptorList;
		}
	}
}
