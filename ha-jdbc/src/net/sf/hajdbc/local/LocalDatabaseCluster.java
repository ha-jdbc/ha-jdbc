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
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import net.sf.hajdbc.ConnectionFactoryProxy;
import net.sf.hajdbc.Database;
import net.sf.hajdbc.DatabaseCluster;
import net.sf.hajdbc.DatabaseClusterDescriptor;
import net.sf.hajdbc.SQLException;

/**
 * @author  Paul Ferraro
 * @version $Revision$
 * @since   1.0
 */
public class LocalDatabaseCluster extends DatabaseCluster
{
	private Set activeDatabaseSet = new LinkedHashSet();
	private LocalDatabaseClusterDescriptor descriptor;
	private ConnectionFactoryProxy connectionFactory;
	
	/**
	 * Constructs a new DatabaseCluster.
	 * @param descriptor
	 * @param databaseMap
	 */
	public LocalDatabaseCluster(LocalDatabaseClusterDescriptor descriptor) throws java.sql.SQLException
	{
		this.descriptor = descriptor;
		
		Map databaseMap = descriptor.getDatabaseMap();
		Map connectionFactoryMap = new HashMap(databaseMap.size());
		
		Iterator databases = databaseMap.values().iterator();
		
		while (databases.hasNext())
		{
			Database database = (Database) databases.next();
			
			connectionFactoryMap.put(database, database.getConnectionFactory());
		}
		
		this.connectionFactory = new ConnectionFactoryProxy(this, connectionFactoryMap);
		
		databases = databaseMap.values().iterator();
		
		while (databases.hasNext())
		{
			Database database = (Database) databases.next();
			
			if (this.isAlive(database))
			{
				this.activeDatabaseSet.add(database);
			}
		}
	}

	public ConnectionFactoryProxy getConnectionFactory()
	{
		return this.connectionFactory;
	}
	
	/**
	 * @param database
	 * @return true if the specified database is active, false otherwise
	 */
	public boolean isAlive(Database database)
	{
		Object connectionFactory = this.connectionFactory.getSQLObject(database);
		
		Connection connection = null;
		
		try
		{
			connection = database.connect(connectionFactory);
			
			Statement statement = connection.createStatement();
			
			statement.execute(this.descriptor.getValidateSQL());

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
	 * Deactivates the specified database.
	 * @param database
	 * @return true if the database was successfully deactivated, false if it was already deactivated
	 */
	public boolean deactivate(Database database)
	{
		return this.removeDatabase(database);
	}

	/**
	 * @see net.sf.hajdbc.DatabaseClusterMBean#getName()
	 */
	public String getName()
	{
		return this.descriptor.getName();
	}

	public DatabaseClusterDescriptor getDescriptor()
	{
		return this.descriptor;
	}
	
	public boolean activate(Database database)
	{
		return this.addDatabase(database);
	}
	
	/**
	 * Returns the first database in the cluster
	 * @return the first database in the cluster
	 * @throws SQLException
	 */
	public Database firstDatabase() throws SQLException
	{
		synchronized (this.activeDatabaseSet)
		{
			if (this.activeDatabaseSet.size() == 0)
			{
				throw new SQLException("No active databases in cluster");
			}
			
			return (Database) this.activeDatabaseSet.iterator().next();
		}
	}
	
	/**
	 * Returns the next database in the cluster
	 * @return the next database in the cluster
	 * @throws SQLException
	 */
	public Database nextDatabase() throws SQLException
	{
		synchronized (this.activeDatabaseSet)
		{
			Database database = this.firstDatabase();
			
			if (this.activeDatabaseSet.size() > 1)
			{
				this.activeDatabaseSet.remove(database);
				
				this.activeDatabaseSet.add(database);
			}
			
			return database;
		}
	}

	/**
	 * A list of active databases in this cluster
	 * @return a list of Database objects
	 * @throws SQLException
	 */
	public List getActiveDatabaseList() throws SQLException
	{
		synchronized (this.activeDatabaseSet)
		{
			if (this.activeDatabaseSet.size() == 0)
			{
				throw new SQLException("No active databases in cluster");
			}
			
			return new ArrayList(this.activeDatabaseSet);
		}
	}

	public String[] getActiveDatabases() throws SQLException
	{
		List databaseList = this.getActiveDatabaseList();
		String[] databases = new String[databaseList.size()];
		
		for (int i = 0; i < databaseList.size(); ++i)
		{
			databases[i] = ((Database) databaseList.get(i)).getId();
		}
		
		return databases;
	}
	
	/**
	 * @see net.sf.hajdbc.DatabaseCluster#getDatabase(java.lang.String)
	 */
	public Database getDatabase(String databaseId) throws java.sql.SQLException
	{
		Database database = (Database) this.descriptor.getDatabaseMap().get(databaseId);
		
		if (database == null)
		{
			throw new SQLException("The database cluster '" + this.getName() + "' does not contain the database '" + databaseId + "'.");
		}
		
		return database;
	}

	/**
	 * @see net.sf.hajdbc.DatabaseCluster#addDatabase(net.sf.hajdbc.Database)
	 */
	public boolean addDatabase(Database database)
	{
		synchronized (this.activeDatabaseSet)
		{
			return this.activeDatabaseSet.add(database);
		}
	}

	/**
	 * @see net.sf.hajdbc.DatabaseCluster#removeDatabase(net.sf.hajdbc.Database)
	 */
	public boolean removeDatabase(Database database)
	{
		synchronized (this.activeDatabaseSet)
		{
			return this.activeDatabaseSet.remove(database);
		}
	}
	
	public boolean isActive(Database database)
	{
		synchronized (this.activeDatabaseSet)
		{
			return this.activeDatabaseSet.contains(database);
		}
	}
}
