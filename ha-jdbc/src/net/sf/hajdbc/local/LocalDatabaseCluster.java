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
import java.util.NoSuchElementException;
import java.util.Set;

import net.sf.hajdbc.Balancer;
import net.sf.hajdbc.ConnectionFactoryProxy;
import net.sf.hajdbc.Database;
import net.sf.hajdbc.DatabaseCluster;
import net.sf.hajdbc.Messages;
import net.sf.hajdbc.SQLException;
import net.sf.hajdbc.SynchronizationStrategy;
import net.sf.hajdbc.SynchronizationStrategyDescriptor;

/**
 * @author  Paul Ferraro
 * @version $Revision$
 * @since   1.0
 */
public class LocalDatabaseCluster extends DatabaseCluster
{
	private String id;
	private String validateSQL;
	private Map databaseMap;
	private Balancer balancer;
	private ConnectionFactoryProxy connectionFactory;
	private Map synchronizationStrategyMap;
	
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
		
		Map connectionFactoryMap = new HashMap(size);
		
		this.databaseMap = new HashMap(size);
		
		Iterator databases = descriptor.getDatabaseList().iterator();
		
		while (databases.hasNext())
		{
			Database database = (Database) databases.next();
			
			this.databaseMap.put(database.getId(), database);

			connectionFactoryMap.put(database, database.createConnectionFactory());
		}
		
		this.connectionFactory = new ConnectionFactoryProxy(this, connectionFactoryMap);
		
		List strategyList = descriptor.getSynchronizationStrategyList();
		
		this.synchronizationStrategyMap = new HashMap(strategyList.size());
		
		Iterator strategies = strategyList.iterator();
		
		try
		{
			while (strategies.hasNext())
			{
				SynchronizationStrategyDescriptor strategy = (SynchronizationStrategyDescriptor) strategies.next();
				
				this.synchronizationStrategyMap.put(strategy.getId(), strategy.createSynchronizationStrategy());
			}
			
			this.balancer = (Balancer) descriptor.getBalancerClass().newInstance();
		}
		catch (Exception e)
		{
			throw new SQLException(e);
		}
	}

	public void init()
	{
		Iterator databases = this.databaseMap.values().iterator();
		
		while (databases.hasNext())
		{
			Database database = (Database) databases.next();
			
			if (this.isAlive(database))
			{
				this.activate(database);
			}
		}
	}
	
	/**
	 * @see net.sf.hajdbc.DatabaseCluster#getConnectionFactory()
	 */
	public ConnectionFactoryProxy getConnectionFactory()
	{
		return this.connectionFactory;
	}
	
	/**
	 * @see net.sf.hajdbc.DatabaseCluster#isAlive(net.sf.hajdbc.Database)
	 */
	public boolean isAlive(Database database)
	{
		Object connectionFactory = this.connectionFactory.getSQLObject(database);
		
		Connection connection = null;
		
		try
		{
			connection = database.connect(connectionFactory);
			
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
		return this.balancer.remove(database);
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
		return this.balancer.add(database);
	}
	
	/**
	 * @see net.sf.hajdbc.DatabaseCluster#firstDatabase()
	 */
	public Database firstDatabase() throws SQLException
	{
		try
		{
			return this.balancer.first();
		}
		catch (NoSuchElementException e)
		{
			throw new SQLException(Messages.getMessage(Messages.NO_ACTIVE_DATABASES, this));
		}
	}
	
	/**
	 * @see net.sf.hajdbc.DatabaseCluster#nextDatabase()
	 */
	public Database nextDatabase() throws SQLException
	{
		try
		{
			return this.balancer.next();
		}
		catch (NoSuchElementException e)
		{
			throw new SQLException(Messages.getMessage(Messages.NO_ACTIVE_DATABASES, this));
		}
	}

	/**
	 * @see net.sf.hajdbc.DatabaseCluster#getDatabaseList()
	 */
	public Database[] getDatabases() throws SQLException
	{
		Database[] databases = this.balancer.toArray();
		
		if (databases.length == 0)
		{
			throw new SQLException(Messages.getMessage(Messages.NO_ACTIVE_DATABASES, this));
		}
		
		return databases;
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
	 * @see net.sf.hajdbc.DatabaseCluster#isActive(net.sf.hajdbc.Database)
	 */
	public boolean isActive(Database database)
	{
		return this.balancer.contains(database);
	}

	/**
	 * @see net.sf.hajdbc.DatabaseCluster#getSynchronizationStrategy(java.lang.String)
	 */
	public SynchronizationStrategy getSynchronizationStrategy(String id) throws java.sql.SQLException
	{
		SynchronizationStrategy strategy = (SynchronizationStrategy) this.synchronizationStrategyMap.get(id);
		
		if (strategy == null)
		{
			throw new SQLException(Messages.getMessage(Messages.INVALID_SYNC_STRATEGY, new Object[] { this, id }));
		}
		
		return strategy;
	}

	/**
	 * @see net.sf.hajdbc.DatabaseCluster#getSynchronizationStrategies()
	 */
	public Collection getSynchronizationStrategies()
	{
		return this.synchronizationStrategyMap.keySet();
	}
}
