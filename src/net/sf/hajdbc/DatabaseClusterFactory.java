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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.ResourceBundle;

import net.sf.hajdbc.sql.DataSourceDatabaseCluster;
import net.sf.hajdbc.sql.DriverDatabaseCluster;

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
	private static final String CONFIGURATION_PROPERTY = "ha-jdbc.configuration";	
	private static final String DEFAULT_RESOURCE = "ha-jdbc.xml";
	
	static Logger logger = LoggerFactory.getLogger(DatabaseClusterFactory.class);
	
	private static DatabaseClusterFactory instance = null;
	private static ResourceBundle resource = ResourceBundle.getBundle(DatabaseClusterFactory.class.getName());
		
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
			
			DatabaseClusterFactory factory = DatabaseClusterFactory.class.cast(context.unmarshalDocument(inputStream, null));

			factory.url = url;
			
			return factory;
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
	
	private Map<String, DriverDatabaseCluster> driverDatabaseClusterMap = new HashMap<String, DriverDatabaseCluster>();
	private Map<String, DataSourceDatabaseCluster> dataSourceDatabaseClusterMap = new HashMap<String, DataSourceDatabaseCluster>();
	private Map<String, SynchronizationStrategy> synchronizationStrategyMap = new HashMap<String, SynchronizationStrategy>();
	private DatabaseClusterDecorator decorator;
	private URL url;
	
	private DatabaseClusterFactory()
	{
		// Do nothing
	}

	public Map<String, DriverDatabaseCluster> getDriverDatabaseClusterMap()
	{
		return this.driverDatabaseClusterMap;
	}
	
	public Map<String, DataSourceDatabaseCluster> getDataSourceDatabaseClusterMap()
	{
		return this.dataSourceDatabaseClusterMap;
	}
	
	public Map<String, SynchronizationStrategy> getSynchronizationStrategyMap()
	{
		return this.synchronizationStrategyMap;
	}
	
	/**
	 * Exports the current HA-JDBC configuration.
	 */
	public synchronized void exportConfig()
	{
		File file = null;
		WritableByteChannel outputChannel = null;
		FileChannel fileChannel = null;
		
		try
		{
			file = this.exportToFile();
			
			fileChannel = new FileInputStream(file).getChannel();
			
			// We cannot use URLConnection for files becuase Sun's implementation does not support output.
			if (this.url.getProtocol().equals("file"))
			{
				outputChannel = new FileOutputStream(new File(this.url.getPath())).getChannel();
			}
			else
			{
				URLConnection connection = this.url.openConnection();
				
				connection.connect();
				
				outputChannel = Channels.newChannel(connection.getOutputStream());
			}
			
			fileChannel.transferTo(0, file.length(), outputChannel);
		}
		catch (Exception e)
		{
			logger.warn(Messages.getMessage(Messages.CONFIG_STORE_FAILED, this.url), e);
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
					logger.warn(e.getMessage(), e);
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
					logger.warn(e.getMessage(), e);
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

	<D> void addDatabaseCluster(DatabaseCluster<D> databaseCluster) throws Exception
	{
		if (this.decorator != null)
		{
			this.decorator.decorate(databaseCluster);			
		}
		
		if (DriverDatabaseCluster.class.isInstance(databaseCluster))
		{
			this.driverDatabaseClusterMap.put(databaseCluster.getId(), DriverDatabaseCluster.class.cast(databaseCluster));
		}
		else
		{
			this.dataSourceDatabaseClusterMap.put(databaseCluster.getId(), DataSourceDatabaseCluster.class.cast(databaseCluster));
		}
		
		try
		{
			databaseCluster.start();
		}
		catch (Exception e)
		{
			this.stop();
			
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
	
	Iterator<DatabaseCluster<?>> getDatabaseClusters()
	{
		List<DatabaseCluster<?>> list = new ArrayList<DatabaseCluster<?>>(this.driverDatabaseClusterMap.size() + this.dataSourceDatabaseClusterMap.size());
		
		list.addAll(this.driverDatabaseClusterMap.values());
		list.addAll(this.dataSourceDatabaseClusterMap.values());
		
		return list.iterator();
	}
	
	private void stop()
	{
		for (DriverDatabaseCluster cluster: this.driverDatabaseClusterMap.values())
		{
			cluster.stop();
		}
		
		for (DataSourceDatabaseCluster cluster: this.dataSourceDatabaseClusterMap.values())
		{
			cluster.stop();
		}
	}
	
	@Override
	protected void finalize()
	{
		this.stop();
	}
}
