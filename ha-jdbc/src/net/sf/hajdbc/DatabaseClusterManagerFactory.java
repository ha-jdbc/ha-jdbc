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
public class DatabaseClusterManagerFactory
{
	private static final String SYSTEM_PROPERTY = "ha-jdbc.configuration";
	private static final String DEFAULT_RESOURCE = "ha-jdbc.xml";
	
	private static Log log = LogFactory.getLog(DatabaseClusterManagerFactory.class);
	
	private static DatabaseClusterManager databaseClusterManager = null;
	
	public static synchronized DatabaseClusterManager getClusterManager() throws SQLException
	{
		if (databaseClusterManager == null)
		{
			databaseClusterManager = loadDatabaseClusterManager();
		}
		
		return databaseClusterManager;
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
	
	private static DatabaseClusterManager loadDatabaseClusterManager() throws SQLException
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
			
			Configuration configuration = (Configuration) context.unmarshalDocument(new InputStreamReader(inputStream));
			
			return configuration.getDatabaseClusterManager();
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
	
	private static class Configuration
	{
		private DatabaseClusterManager databaseClusterManager;
		
		public DatabaseClusterManager getDatabaseClusterManager()
		{
			return this.databaseClusterManager;
		}
		
		public void setDatabaseClusterManager(DatabaseClusterManager databaseClusterManager)
		{
			this.databaseClusterManager = databaseClusterManager;
		}
	}
}
