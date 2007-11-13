/*
 * HA-JDBC: High-Availability JDBC
 * Copyright (c) 2004-2007 Paul Ferraro
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

import java.lang.management.ManagementFactory;
import java.lang.reflect.InvocationTargetException;
import java.net.MalformedURLException;
import java.net.URL;
import java.sql.SQLException;
import java.text.MessageFormat;
import java.util.Hashtable;
import java.util.ResourceBundle;

import javax.management.JMException;
import javax.management.MBeanServer;
import javax.management.MBeanServerInvocationHandler;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import net.sf.hajdbc.util.SQLExceptionFactory;

/**
 * @author Paul Ferraro
 */
public class DatabaseClusterFactory
{
	private static final String CONFIGURATION_PROPERTY = "ha-jdbc.configuration"; //$NON-NLS-1$
	private static final String DEFAULT_RESOURCE = "ha-jdbc-{0}.xml"; //$NON-NLS-1$
	private static final String MBEAN_CLUSTER_KEY = "cluster"; //$NON-NLS-1$
	private static final String MBEAN_DATABASE_KEY = "database"; //$NON-NLS-1$
	private static final String VERSION = "version"; //$NON-NLS-1$
	
	private static ResourceBundle resource = ResourceBundle.getBundle(DatabaseClusterFactory.class.getName());
	
	/**
	 * Convenience method for constructing a standardized mbean ObjectName for this cluster.
	 * @param clusterId a cluster identifier
	 * @return an ObjectName for this cluster
	 * @throws MalformedObjectNameException if the ObjectName could not be constructed
	 */
	public static ObjectName getObjectName(String clusterId) throws MalformedObjectNameException
	{
		return ObjectName.getInstance(getDomain(), createProperties(clusterId));
	}

	/**
	 * Convenience method for constructing a standardized mbean ObjectName for this database.
	 * @param clusterId a cluster identifier
	 * @param databaseId a database identifier
	 * @return an ObjectName for this cluster
	 * @throws MalformedObjectNameException if the ObjectName could not be constructed
	 */
	public static ObjectName getObjectName(String clusterId, String databaseId) throws MalformedObjectNameException
	{
		Hashtable<String, String> properties = createProperties(clusterId);
		
		properties.put(MBEAN_DATABASE_KEY, databaseId);
		
		return ObjectName.getInstance(getDomain(), properties);
	}
	
	private static Hashtable<String, String> createProperties(String clusterId)
	{
		Hashtable<String, String> properties = new Hashtable<String, String>();
		
		properties.put(MBEAN_CLUSTER_KEY, clusterId);
		
		return properties;
	}
	
	private static String getDomain()
	{
		return DatabaseClusterFactory.class.getPackage().getName();
	}
	
	/**
	 * Returns the current HA-JDBC version.
	 * @return a version label
	 */
	public static String getVersion()
	{
		return resource.getString(VERSION);
	}
	
	public static synchronized <C extends DatabaseCluster<?>> C getDatabaseCluster(String id, Class<? extends C> targetClass, Class<C> mbeanInterface, String resource) throws SQLException
	{
		try
		{
			ObjectName name = getObjectName(id);
			
			MBeanServer server = ManagementFactory.getPlatformMBeanServer();
			
			if (!server.isRegistered(name))
			{
				URL url = getResourceURL((resource == null) ? MessageFormat.format(System.getProperty(CONFIGURATION_PROPERTY, DEFAULT_RESOURCE), id) : resource);
				
				C cluster = targetClass.getConstructor(String.class, URL.class).newInstance(id, url);
				
				server.registerMBean(cluster, name);
			}
			
			return MBeanServerInvocationHandler.newProxyInstance(server, name, mbeanInterface, false);
		}
		catch (JMException e)
		{
			throw SQLExceptionFactory.createSQLException(e);
		}
		catch (InstantiationException e)
		{
			throw SQLExceptionFactory.createSQLException(e);
		}
		catch (IllegalAccessException e)
		{
			throw SQLExceptionFactory.createSQLException(e);
		}
		catch (NoSuchMethodException e)
		{
			throw SQLExceptionFactory.createSQLException(e);
		}
		catch (InvocationTargetException e)
		{
			throw SQLExceptionFactory.createSQLException(e);
		}
	}
	
	/**
	 * Algorithm for searching class loaders for HA-JDBC url.
	 * @param resource a resource name
	 * @return a URL for the HA-JDBC configuration resource
	 */
	private static URL getResourceURL(String resource) throws SQLException
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
				throw new SQLException(Messages.getMessage(Messages.CONFIG_NOT_FOUND, resource));
			}
			
			return url;
		}
	}
}
