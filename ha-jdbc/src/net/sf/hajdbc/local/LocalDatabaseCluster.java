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
package net.sf.hajdbc.local;

import java.sql.Connection;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.prefs.BackingStoreException;
import java.util.prefs.Preferences;

import net.sf.hajdbc.AbstractDatabaseCluster;
import net.sf.hajdbc.Balancer;
import net.sf.hajdbc.Database;
import net.sf.hajdbc.Dialect;
import net.sf.hajdbc.Messages;
import net.sf.hajdbc.SQLException;
import net.sf.hajdbc.SynchronizationStrategy;
import net.sf.hajdbc.util.concurrent.DaemonThreadFactory;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @author  Paul Ferraro
 * @version $Revision$
 * @since   1.0
 */
public class LocalDatabaseCluster extends AbstractDatabaseCluster
{
	private static final String DELIMITER = ",";
	
	private static Preferences preferences = Preferences.userNodeForPackage(LocalDatabaseCluster.class);
	private static Log log = LogFactory.getLog(LocalDatabaseCluster.class);
	
	private String id;
	private Map<String, Database> databaseMap = new HashMap<String, Database>();
	private Balancer balancer;
	private SynchronizationStrategy defaultSynchronizationStrategy;
	private Map<Database, Object> connectionFactoryMap = new HashMap<Database, Object>();
	private ThreadPoolExecutor executor = ThreadPoolExecutor.class.cast(Executors.newCachedThreadPool(new DaemonThreadFactory()));
	private Dialect dialect;
	
	/**
	 * @see net.sf.hajdbc.DatabaseCluster#loadState()
	 */
	public String[] loadState() throws java.sql.SQLException
	{
		try
		{
			preferences.sync();
			
			String state = preferences.get(this.id, null);
			
			if (state == null)
			{
				return null;
			}
			
			if (state.length() == 0)
			{
				return new String[0];
			}
			
			String[] databases = state.split(DELIMITER);
			
			// Validate persisted cluster state
			for (String id: databases)
			{
				if (!this.databaseMap.containsKey(id))
				{
					// Persisted cluster state is invalid!
					preferences.remove(this.id);					
					preferences.flush();
					
					return null;
				}
			}
			
			return databases;
		}
		catch (BackingStoreException e)
		{
			throw new SQLException(Messages.getMessage(Messages.CLUSTER_STATE_LOAD_FAILED, this), e);
		}
	}

	/**
	 * @see net.sf.hajdbc.DatabaseCluster#getConnectionFactoryMap()
	 */
	public Map<Database, ?> getConnectionFactoryMap()
	{
		return this.connectionFactoryMap;
	}
	
	/**
	 * @see net.sf.hajdbc.DatabaseCluster#isAlive(net.sf.hajdbc.Database)
	 */
	public boolean isAlive(Database database)
	{
		Connection connection = null;
		
		try
		{
			connection = database.connect(this.connectionFactoryMap.get(database));
			
			Statement statement = connection.createStatement();
			
			statement.execute(this.dialect.getSimpleSQL());

			statement.close();
			
			return true;
		}
		catch (java.sql.SQLException e)
		{
			return false;
		}
		finally
		{
			if (connection != null)
			{
				try
				{
					connection.close();
				}
				catch (java.sql.SQLException e)
				{
					// Ignore
				}
			}
		}
	}
	
	/**
	 * @see net.sf.hajdbc.DatabaseCluster#deactivate(net.sf.hajdbc.Database)
	 */
	public boolean deactivate(Database database)
	{
		boolean removed = this.balancer.remove(database);
		
		if (removed)
		{
			this.storeState();
		}
		
		return removed;
	}

	/**
	 * @see net.sf.hajdbc.DatabaseClusterMBean#getId()
	 */
	public String getId()
	{
		return this.id;
	}
	
	/**
	 * @see net.sf.hajdbc.DatabaseCluster#activate(net.sf.hajdbc.Database)
	 */
	public boolean activate(Database database)
	{
		boolean added = this.balancer.add(database);
		
		if (added)
		{
			this.storeState();
		}
		
		return added;
	}
	
	private void storeState()
	{
		StringBuilder builder = new StringBuilder();
		
		Iterator<Database> databases = this.balancer.list().iterator();
		
		while (databases.hasNext())
		{
			builder.append(databases.next().getId());
			
			if (databases.hasNext())
			{
				builder.append(DELIMITER);
			}
		}
		
		preferences.put(this.id, builder.toString());
		
		try
		{
			preferences.flush();
		}
		catch (BackingStoreException e)
		{
			log.warn(Messages.getMessage(Messages.CLUSTER_STATE_STORE_FAILED, this), e);
		}
	}
	
	/**
	 * @see net.sf.hajdbc.DatabaseClusterMBean#getActiveDatabases()
	 */
	public Collection<String> getActiveDatabases()
	{
		return this.getDatabaseIds(this.balancer.list());
	}

	/**
	 * @see net.sf.hajdbc.DatabaseClusterMBean#getInactiveDatabases()
	 */
	public Collection<String> getInactiveDatabases()
	{
		Set<Database> databaseSet = new HashSet<Database>(this.databaseMap.values());

		databaseSet.removeAll(this.balancer.list());

		return this.getDatabaseIds(databaseSet);
	}
	
	private List<String> getDatabaseIds(Collection<Database> databases)
	{
		List<String> databaseList = new ArrayList<String>(databases.size());
		
		for (Database database: databases)
		{
			databaseList.add(database.getId());
		}
		
		return databaseList;
	}
	
	/**
	 * @see net.sf.hajdbc.DatabaseCluster#getDatabase(java.lang.String)
	 */
	public Database getDatabase(String databaseId) throws java.sql.SQLException
	{
		Database database = this.databaseMap.get(databaseId);
		
		if (database == null)
		{
			throw new SQLException(Messages.getMessage(Messages.INVALID_DATABASE, databaseId, this));
		}
		
		return database;
	}

	/**
	 * @see net.sf.hajdbc.DatabaseCluster#getDefaultSynchronizationStrategy()
	 */
	public SynchronizationStrategy getDefaultSynchronizationStrategy()
	{
		return this.defaultSynchronizationStrategy;
	}
	
	/**
	 * @see net.sf.hajdbc.DatabaseCluster#getBalancer()
	 */
	public Balancer getBalancer()
	{
		return this.balancer;
	}

	/**
	 * @see net.sf.hajdbc.DatabaseCluster#getExecutor()
	 */
	public ExecutorService getExecutor()
	{
		return this.executor;
	}

	/**
	 * @see net.sf.hajdbc.DatabaseCluster#getDialect()
	 */
	public Dialect getDialect()
	{
		return this.dialect;
	}
	
	void addDatabase(Database database)
	{
		this.databaseMap.put(database.getId(), database);
	}
	
	void createConnectionFactories() throws java.sql.SQLException
	{
		try
		{
			for (Database database: this.databaseMap.values())
			{
				this.connectionFactoryMap.put(database, database.createConnectionFactory());
			}
		}
		catch (java.sql.SQLException e)
		{
			// JiBX will mask this exception, so log it here
			log.error(e.getMessage(), e);
			
			throw e;
		}
	}
	
	void setMinThreads(int size)
	{
		this.executor.setCorePoolSize(size);
	}
	
	void setMaxThreads(int size)
	{
		this.executor.setMaximumPoolSize(size);
	}
	
	void setMaxIdle(int seconds)
	{
		this.executor.setKeepAliveTime(seconds, TimeUnit.SECONDS);
	}
}
