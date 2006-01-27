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

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * @author  Paul Ferraro
 * @since   1.0
 */
public abstract class AbstractDatabaseCluster implements DatabaseCluster
{
	private static final String SYNC_LOCK_HANDLE = "";
	
	private static Log log = LogFactory.getLog(AbstractDatabaseCluster.class);
	
	/**
	 * @see net.sf.hajdbc.DatabaseClusterMBean#isAlive(java.lang.String)
	 */
	public final boolean isAlive(String id)
	{
		return this.isAlive(this.getDatabase(id));
	}

	/**
	 * @see net.sf.hajdbc.DatabaseClusterMBean#deactivate(java.lang.String)
	 */
	public final void deactivate(String databaseId)
	{
		if (this.deactivate(this.getDatabase(databaseId)))
		{
			log.info(Messages.getMessage(Messages.DATABASE_DEACTIVATED, databaseId, this));
		}
	}

	/**
	 * @see net.sf.hajdbc.DatabaseClusterMBean#activate(java.lang.String)
	 */
	public final void activate(String databaseId) throws Exception
	{
		this.activate(databaseId, this.getDefaultSynchronizationStrategy());
	}
	
	/**
	 * @see net.sf.hajdbc.DatabaseClusterMBean#activate(java.lang.String, java.lang.String)
	 */
	public final void activate(String databaseId, String strategyId) throws Exception
	{
		this.activate(databaseId, DatabaseClusterFactory.getInstance().getSynchronizationStrategy(strategyId));
	}
	
	/**
	 * Handles a failure caused by the specified cause on the specified database.
	 * If the database is not alive, then it is deactivated, otherwise an exception is thrown back to the caller.
	 * @param database a database descriptor
	 * @param cause the cause of the failure
	 * @throws java.sql.SQLException if the database is alive
	 */
	public final void handleFailure(Database database, java.sql.SQLException cause) throws java.sql.SQLException
	{
		if (this.isAlive(database))
		{
			throw cause;
		}
		
		if (this.deactivate(database))
		{
			log.error(Messages.getMessage(Messages.DATABASE_DEACTIVATED, database, this), cause);
		}
	}
	
	/**
	 * @see java.lang.Object#toString()
	 */
	public final String toString()
	{
		return this.getId();
	}
	
	/**
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	public final boolean equals(Object object)
	{
		DatabaseCluster databaseCluster = (DatabaseCluster) object;
		
		return this.getId().equals(databaseCluster.getId());
	}
	
	private void activate(String databaseId, SynchronizationStrategy strategy) throws Exception
	{
		try
		{
			if (this.activate(this.getDatabase(databaseId), strategy))
			{
				log.info(Messages.getMessage(Messages.DATABASE_ACTIVATED, databaseId, this));
			}
		}
		catch (java.sql.SQLException e)
		{
			String message = Messages.getMessage(Messages.DATABASE_ACTIVATE_FAILED, databaseId, this);
			
			log.error(message, e);
			
			throw new Exception(message);
		}
	}
	
	private boolean activate(Database inactiveDatabase, SynchronizationStrategy strategy) throws java.sql.SQLException
	{
		if (this.getBalancer().contains(inactiveDatabase))
		{
			return false;
		}
		
		List<Database> databaseList = this.getBalancer().list();
		
		if (databaseList.isEmpty())
		{
			return this.activate(inactiveDatabase);
		}
		
		if (!this.tryLock(SYNC_LOCK_HANDLE))
		{
			throw new IllegalStateException(Messages.getMessage(Messages.DATABASE_ACTIVATION_ALREADY_RUNNING, inactiveDatabase, this));
		}
		
		Database activeDatabase = databaseList.get(0);
		
		Connection inactiveConnection = null;
		Connection activeConnection = null;

		List<Connection> connectionList = new ArrayList<Connection>(databaseList.size());
		
		try
		{
			Map<Database, ?> connectionFactoryMap = this.getConnectionFactoryMap();
			
			inactiveConnection = inactiveDatabase.connect(connectionFactoryMap.get(inactiveDatabase));
			
			Map<String, List<String>> schemaMap = new HashMap<String, List<String>>();
			
			DatabaseMetaData metaData = inactiveConnection.getMetaData();

			ResultSet resultSet = metaData.getTables(null, null, "%", new String[] { "TABLE" });
			
			while (resultSet.next())
			{
				String table = resultSet.getString("TABLE_NAME");
				String schema = resultSet.getString("TABLE_SCHEM");

				List<String> tableList = schemaMap.get(schema);
				
				if (tableList == null)
				{
					tableList = new LinkedList();
					
					schemaMap.put(schema, tableList);
				}
				
				tableList.add(table);
			}
			
			resultSet.close();

			Dialect dialect = this.getDialect();
			
			activeConnection = activeDatabase.connect(connectionFactoryMap.get(activeDatabase));
			
			if (strategy.requiresTableLocking())
			{
				// Lock all tables on all active databases
				for (Database database: databaseList)
				{
					Connection connection = database.equals(activeDatabase) ? activeConnection : database.connect(connectionFactoryMap.get(database));
					
					connectionList.add(connection);
					
					connection.setAutoCommit(false);
					connection.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
					
					Statement statement = connection.createStatement();
					
					for (Map.Entry<String, List<String>> schemaMapEntry: schemaMap.entrySet())
					{
						String schema = schemaMapEntry.getKey();
						
						for (String table: schemaMapEntry.getValue())
						{
							statement.executeUpdate(dialect.getLockTableSQL(metaData, schema, table));
						}
					}
					
					statement.close();
				}
			}
			
			log.info(Messages.getMessage(Messages.DATABASE_SYNC_START, inactiveDatabase));

			strategy.synchronize(inactiveConnection, activeConnection, schemaMap, dialect);
			
			log.info(Messages.getMessage(Messages.DATABASE_SYNC_END, inactiveDatabase));
	
			this.activate(inactiveDatabase);
			
			if (strategy.requiresTableLocking())
			{
				// Release table locks
				for (Connection connection: connectionList)
				{
					connection.rollback();
					connection.setAutoCommit(true);
				}
			}
			
			return true;
		}
		finally
		{
			this.close(activeConnection, activeDatabase);
			this.close(inactiveConnection, inactiveDatabase);
			
			for (int i = 0; i < connectionList.size(); ++i)
			{
				this.close(connectionList.get(i), databaseList.get(i));
			}
			
			this.unlock(SYNC_LOCK_HANDLE);
		}
	}
	
	private void close(Connection connection, Database database)
	{
		if (connection != null)
		{
			try
			{
				if (!connection.isClosed())
				{
					connection.close();
				}
			}
			catch (java.sql.SQLException e)
			{
				log.warn(e.getMessage(), e);
			}
		}
	}
	
	/**
	 * @see net.sf.hajdbc.DatabaseCluster#init()
	 */
	public void init() throws java.sql.SQLException
	{
		String[] databases = this.loadState();

		if (databases != null)
		{
			for (String id: databases)
			{
				Database database = this.getDatabase(id);
				
				this.activate(database);
			}
		}
		else
		{
			for (String id: this.getInactiveDatabases())
			{
				Database database = this.getDatabase(id);
				
				if (this.isAlive(database))
				{
					this.activate(database);
				}
			}
		}
	}
}
