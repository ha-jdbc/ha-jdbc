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
import java.util.Map;

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
	
	private static Log log = LogFactory.getLog(DatabaseClusterManagerFactory.class);
	
	private static DatabaseClusterFactory instance = null;
	
	private Configuration configuration;
	
	public static synchronized DatabaseClusterFactory getInstance() throws java.sql.SQLException
	{
		if (instance == null)
		{
			instance = new DatabaseClusterFactory();
		}
		
		return instance;
	}
	
	private DatabaseClusterFactory() throws java.sql.SQLException
	{
		String resourceName = System.getProperty(SYSTEM_PROPERTY, DEFAULT_RESOURCE);
		
		URL resourceURL = getResourceURL(resourceName);
		
		if (resourceURL == null)
		{
			throw new SQLException("Failed to locate database cluster configuration file: " + resourceName);
		}
		
		InputStream inputStream = null;
		
		try
		{
			inputStream = resourceURL.openStream();
			
			IBindingFactory factory = BindingDirectory.getFactory(Configuration.class);
			IUnmarshallingContext context = factory.createUnmarshallingContext();
			
			this.configuration = (Configuration) context.unmarshalDocument(new InputStreamReader(inputStream));
		}
		catch (IOException e)
		{
			throw new SQLException("Failed to read " + resourceURL, e);
		}
		catch (JiBXException e)
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
	
	public DatabaseCluster getDatabaseCluster()
	{
		return null;
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
				url = DatabaseClusterManagerFactory.class.getClassLoader().getResource(resourceName);
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
		private DatabaseClusterListener listener = new LocalDatabaseClusterListener();
		private Map descriptorMap = new HashMap();
		
		public DatabaseClusterListener getListener()
		{
			return this.listener;
		}
		
		public Map getDescriptorMap()
		{
			return this.descriptorMap;
		}
		
		private void addDescriptor(Object object)
		{
			DatabaseClusterDescriptor descriptor = (DatabaseClusterDescriptor) object;
			
			this.descriptorMap.put(descriptor.getName(), descriptor);
		}
	}
}
