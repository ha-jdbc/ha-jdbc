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
package net.sf.hajdbc.local;

import java.sql.Connection;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
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
import net.sf.hajdbc.ConnectionFactory;
import net.sf.hajdbc.Database;
import net.sf.hajdbc.Messages;
import net.sf.hajdbc.SQLException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * @author  Paul Ferraro
 * @version $Revision$
 * @since   1.0
 */
public class LocalDatabaseCluster extends AbstractDatabaseCluster
{
	private static final String CLUSTER_STATE_DELIMITER = ",";
	
	private static Preferences preferences = Preferences.userNodeForPackage(LocalDatabaseCluster.class);
	private static Log log = LogFactory.getLog(LocalDatabaseCluster.class);
	
	private String id;
	private String validateSQL;
	private Map databaseMap;
	private Balancer balancer;
	private String defaultSynchronizationStrategy;
	private ConnectionFactory connectionFactory;
	
	/**
	 * Constructs a new LocalDatabaseCluster.
	 * @param descriptor a local database cluster descriptor
	 * @throws java.sql.SQLException
	 */
	public LocalDatabaseCluster(LocalDatabaseClusterDescriptor descriptor) throws java.sql.SQLException
	{
		this.id = descriptor.getId();
		this.validateSQL = descriptor.getValidateSQL();
		
		List databaseList = descriptor.getDatabaseList();
		int size = databaseList.size();
		
		this.databaseMap = new HashMap(size);
		Map connectionFactoryMap = new HashMap(size);
		
		Iterator databases = descriptor.getDatabaseList().iterator();
		
		while (databases.hasNext())
		{
			Database database = (Database) databases.next();
			
			this.databaseMap.put(database.getId(), database);

			connectionFactoryMap.put(database, database.createConnectionFactory());
		}

		this.connectionFactory = new ConnectionFactory(this, connectionFactoryMap);
		
		try
		{
			this.balancer = (Balancer) descriptor.getBalancerClass().newInstance();
		}
		catch (Exception e)
		{
			throw new SQLException(e);
		}
	}

	/**
	 * @see net.sf.hajdbc.DatabaseCluster#init()
	 */
	public void init()
	{
		if (!this.restoreClusterState())
		{
			Iterator databases = this.databaseMap.values().iterator();
			
			while (databases.hasNext())
			{
				Database database = (Database) databases.next();
				
				if (this.isAlive(database))
				{
					this.balancer.add(database);
				}
			}
		}
		
		this.persistClusterState();
	}
	
	/**
	 * @see net.sf.hajdbc.DatabaseCluster#getConnectionFactory()
	 */
	public ConnectionFactory getConnectionFactory()
	{
		return this.connectionFactory;
	}
	
	/**
	 * @see net.sf.hajdbc.DatabaseCluster#isAlive(net.sf.hajdbc.Database)
	 */
	public boolean isAlive(Database database)
	{
		Connection connection = null;
		
		try
		{
			connection = database.connect(this.connectionFactory.getObject(database));
			
			Statement statement = connection.createStatement();
			
			statement.execute(this.validateSQL);

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
			this.persistClusterState();
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
			this.persistClusterState();
		}
		
		return added;
	}
	
	private boolean restoreClusterState()
	{
		try
		{
			preferences.sync();
			
			String state = preferences.get(this.id, null);
			
			if (state == null) return false;
			
			String[] databases = state.split(CLUSTER_STATE_DELIMITER);
			
			for (int i = 0; i < databases.length; ++i)
			{
				try
				{
					Database database = this.getDatabase(databases[i]);
					
					this.balancer.add(database);
				}
				catch (java.sql.SQLException e)
				{
					// Ignore - database is no longer in this cluster
				}
			}
			
			return true;
		}
		catch (BackingStoreException e)
		{
			log.warn(Messages.getMessage(Messages.CLUSTER_STATE_LOAD_FAILED, this), e);
			
			return false;
		}
	}
	
	private void persistClusterState()
	{
		StringBuffer buffer = new StringBuffer();
		Database[] databases = this.balancer.toArray();
		
		for (int i = 0; i < databases.length; ++i)
		{
			if (i > 0)
			{
				buffer.append(CLUSTER_STATE_DELIMITER);
			}
			
			buffer.append(databases[i].getId());
		}
		
		preferences.put(this.id, buffer.toString());
		
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
	public Collection getActiveDatabases()
	{
		return this.getDatabaseIds(Arrays.asList(this.balancer.toArray()));
	}

	/**
	 * @see net.sf.hajdbc.DatabaseClusterMBean#getInactiveDatabases()
	 */
	public Collection getInactiveDatabases()
	{
		Set databaseSet = new HashSet(this.databaseMap.values());

		databaseSet.removeAll(Arrays.asList(this.balancer.toArray()));

		return this.getDatabaseIds(databaseSet);
	}
	
	private List getDatabaseIds(Collection databaseCollection)
	{
		List databaseList = new ArrayList(databaseCollection.size());
		
		Iterator databases = databaseCollection.iterator();
		
		while (databases.hasNext())
		{
			Database database = (Database) databases.next();
			
			databaseList.add(database.getId());
		}
		
		return databaseList;
	}
	
	/**
	 * @see net.sf.hajdbc.DatabaseCluster#getDatabase(java.lang.String)
	 */
	public Database getDatabase(String databaseId) throws java.sql.SQLException
	{
		Database database = (Database) this.databaseMap.get(databaseId);
		
		if (database == null)
		{
			throw new SQLException(Messages.getMessage(Messages.INVALID_DATABASE, new Object[] { databaseId, this }));
		}
		
		return database;
	}

	/**
	 * @see net.sf.hajdbc.DatabaseCluster#getDefaultSynchronizationStrategy()
	 */
	public String getDefaultSynchronizationStrategy()
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
}
