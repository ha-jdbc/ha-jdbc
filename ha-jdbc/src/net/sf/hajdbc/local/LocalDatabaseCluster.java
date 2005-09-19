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
import net.sf.hajdbc.SynchronizationStrategy;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

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
	private String validateSQL;
	private Map databaseMap = new HashMap();
	private Balancer balancer;
	private SynchronizationStrategy defaultSynchronizationStrategy;
	private ConnectionFactory connectionFactory;
	
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
			for (int i = 0; i < databases.length; ++i)
			{
				if (!this.databaseMap.containsKey(databases[i]))
				{
					// Persisted cluster state is invalid!
					preferences.remove(this.id);
					
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
		StringBuffer buffer = new StringBuffer();
		Database[] databases = this.balancer.toArray();
		
		for (int i = 0; i < databases.length; ++i)
		{
			if (i > 0)
			{
				buffer.append(DELIMITER);
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
	
	void addDatabase(Database database)
	{
		this.databaseMap.put(database.getId(), database);
	}
	
	void createConnectionFactories() throws java.sql.SQLException
	{
		try
		{
			Map connectionFactoryMap = new HashMap(this.databaseMap.size());
			
			Iterator databases = this.databaseMap.values().iterator();
			
			while (databases.hasNext())
			{
				Database database = (Database) databases.next();
	
				connectionFactoryMap.put(database, database.createConnectionFactory());
			}
	
			this.connectionFactory = new ConnectionFactory(this, connectionFactoryMap);
		}
		catch (java.sql.SQLException e)
		{
			// JiBX will mask this exception, so log it here
			log.error(e.getMessage(), e);
			
			throw e;
		}
	}
}
