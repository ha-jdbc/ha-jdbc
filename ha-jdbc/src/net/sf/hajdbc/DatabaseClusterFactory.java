/*
 * Copyright (c) 2004, Identity Theft 911, LLC.  All rights reserved.
 *
 * $Id$
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

import javax.management.InstanceAlreadyExistsException;
import javax.management.MBeanRegistrationException;
import javax.management.MBeanServer;
import javax.management.MBeanServerFactory;
import javax.management.MalformedObjectNameException;
import javax.management.NotCompliantMBeanException;
import javax.management.ObjectName;

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
	
	private static Log log = LogFactory.getLog(DatabaseClusterFactory.class);
	
	private static DatabaseClusterFactory instance = null;
	
	public static synchronized DatabaseClusterFactory getInstance() throws java.sql.SQLException
	{
		if (instance == null)
		{
			instance = new DatabaseClusterFactory();
		}
		
		return instance;
	}
	
	private Map databaseClusterMap;
	
	private DatabaseClusterFactory() throws java.sql.SQLException
	{
		String resourceName = System.getProperty(SYSTEM_PROPERTY, DEFAULT_RESOURCE);
		
		URL resourceURL = getResourceURL(resourceName);
		
		if (resourceURL == null)
		{
			throw new SQLException("Failed to locate database cluster configuration file: " + resourceName);
		}
		
		InputStream inputStream = null;
		MBeanServer server = null;
		
		try
		{
			inputStream = resourceURL.openStream();
			
			IBindingFactory factory = BindingDirectory.getFactory(Configuration.class);
			IUnmarshallingContext context = factory.createUnmarshallingContext();
			
			Configuration configuration = (Configuration) context.unmarshalDocument(new InputStreamReader(inputStream));

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
				
				DatabaseCluster databaseCluster = new DatabaseCluster(descriptor);
				
				server.registerMBean(databaseCluster, ObjectName.getInstance("net.sf.hajdbc", "id", databaseCluster.getName()));
				
				this.databaseClusterMap.put(descriptor.getName(), databaseCluster);
			}
		}
		catch (IOException e)
		{
			throw new SQLException("Failed to read " + resourceURL, e);
		}
		catch (JiBXException e)
		{
			throw new SQLException("Failed to parse " + resourceURL, e);
		}
		catch (InstanceAlreadyExistsException e)
		{
			throw new SQLException("Failed to parse " + resourceURL, e);
		}
		catch (MBeanRegistrationException e)
		{
			throw new SQLException("Failed to parse " + resourceURL, e);
		}
		catch (NotCompliantMBeanException e)
		{
			throw new SQLException("Failed to parse " + resourceURL, e);
		}
		catch (MalformedObjectNameException e)
		{
			throw new SQLException("Failed to parse " + resourceURL, e);
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
					log.warn("Failed to close " + resourceURL, e);
				}
			}
		}
	}
	
	public DatabaseCluster getDatabaseCluster(String name)
	{
		return (DatabaseCluster) this.databaseClusterMap.get(name);
	}
	
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
	
	private static class Configuration
	{
		private Object listenerDescriptor = null;
		private List descriptorList = new LinkedList();
		
		public Object getListenerDescriptor()
		{
			return this.listenerDescriptor;
		}
		
		public List getDescriptorList()
		{
			return this.descriptorList;
		}
	}
}
