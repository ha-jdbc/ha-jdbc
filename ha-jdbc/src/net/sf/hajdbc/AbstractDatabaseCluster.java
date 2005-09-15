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

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * @author  Paul Ferraro
 * @since   1.0
 */
public abstract class AbstractDatabaseCluster implements DatabaseCluster
{
	private static Log log = LogFactory.getLog(AbstractDatabaseCluster.class);
	
	/**
	 * @see net.sf.hajdbc.DatabaseClusterMBean#isAlive(java.lang.String)
	 */
	public final boolean isAlive(String databaseId) throws java.sql.SQLException
	{
		try
		{
			return this.isAlive(this.getDatabase(databaseId));
		}
		catch (java.sql.SQLException e)
		{
			log.warn(Messages.getMessage(Messages.DATABASE_VALIDATE_FAILED, databaseId), e);
			
			throw e;
		}
	}

	/**
	 * @see net.sf.hajdbc.DatabaseClusterMBean#deactivate(java.lang.String)
	 */
	public final void deactivate(String databaseId) throws java.sql.SQLException
	{
		Object[] args = new Object[] { databaseId, this };
		
		try
		{
			if (this.deactivate(this.getDatabase(databaseId)))
			{
				log.info(Messages.getMessage(Messages.DATABASE_DEACTIVATED, args));
			}
		}
		catch (java.sql.SQLException e)
		{
			log.warn(Messages.getMessage(Messages.DATABASE_DEACTIVATE_FAILED, args), e);
			
			throw e;
		}
	}

	/**
	 * @see net.sf.hajdbc.DatabaseClusterMBean#activate(java.lang.String)
	 */
	public final void activate(String databaseId) throws java.sql.SQLException
	{
		this.activate(databaseId, this.getDefaultSynchronizationStrategy());
	}
	
	/**
	 * @see net.sf.hajdbc.DatabaseClusterMBean#activate(java.lang.String, java.lang.String)
	 */
	public final void activate(String databaseId, String strategyId) throws java.sql.SQLException
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
			log.error(Messages.getMessage(Messages.DATABASE_DEACTIVATED, new Object[] { database, this }), cause);
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
	
	private void activate(String databaseId, SynchronizationStrategy strategy) throws java.sql.SQLException
	{
		Object[] args = new Object[] { databaseId, this };
		
		Database database = this.getDatabase(databaseId);
		
		try
		{
			if (this.activate(database, strategy))
			{
				log.info(Messages.getMessage(Messages.DATABASE_ACTIVATED, args));
			}
		}
		catch (java.sql.SQLException e)
		{
			log.warn(Messages.getMessage(Messages.DATABASE_ACTIVATE_FAILED, args), e);
			
			throw e;
		}
	}
	
	private boolean activate(Database inactiveDatabase, SynchronizationStrategy strategy) throws java.sql.SQLException
	{
		if (this.getBalancer().contains(inactiveDatabase))
		{
			return false;
		}
		
		Database[] databases = this.getBalancer().toArray();
		
		if (databases.length == 0)
		{
			return this.activate(inactiveDatabase);
		}
		
		Connection inactiveConnection = null;
		Connection[] activeConnections = new Connection[databases.length];
		
		try
		{
			inactiveConnection = inactiveDatabase.connect(this.getConnectionFactory().getObject(inactiveDatabase));
			
			List tableList = new LinkedList();
			
			DatabaseMetaData databaseMetaData = inactiveConnection.getMetaData();
			String quote = databaseMetaData.getIdentifierQuoteString();
			ResultSet resultSet = databaseMetaData.getTables(null, null, "%", new String[] { "TABLE" });
			
			while (resultSet.next())
			{
				String table = resultSet.getString("TABLE_NAME");
				tableList.add(table);
			}
			
			resultSet.close();

			// Open connections to all active databases
			for (int i = 0; i < databases.length; ++i)
			{
				Database activeDatabase = databases[i];
				
				activeConnections[i] = activeDatabase.connect(this.getConnectionFactory().getObject(activeDatabase));
			}
			
			// Lock all tables on all active databases
			for (int i = 0; i < activeConnections.length; ++i)
			{
				activeConnections[i].setAutoCommit(false);
				activeConnections[i].setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
				
				Statement statement = activeConnections[i].createStatement();
				Iterator tables = tableList.iterator();
				
				while (tables.hasNext())
				{
					String table = (String) tables.next();
					
					statement.execute("SELECT count(*) FROM " + quote + table + quote);
				}
				
				statement.close();
			}
			
			log.info(Messages.getMessage(Messages.DATABASE_SYNC_START, inactiveDatabase));

			strategy.synchronize(inactiveConnection, activeConnections[0], tableList);
			
			log.info(Messages.getMessage(Messages.DATABASE_SYNC_END, inactiveDatabase));
	
			this.activate(inactiveDatabase);
			
			// Release table locks
			for (int i = 0; i < activeConnections.length; ++i)
			{
				activeConnections[i].rollback();
			}
			
			return true;
		}
		finally
		{
			this.close(inactiveConnection, inactiveDatabase);
			
			for (int i = 0; i < activeConnections.length; ++i)
			{
				this.close(activeConnections[i], databases[i]);
			}
		}
	}
	
	private void close(Connection connection, Database database)
	{
		if (connection != null)
		{
			try
			{
				connection.close();
			}
			catch (java.sql.SQLException e)
			{
				log.warn(Messages.getMessage(Messages.CONNECTION_CLOSE_FAILED, database), e);
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
			for (int i = 0; i < databases.length; ++i)
			{
				Database database = this.getDatabase(databases[i]);
				
				this.activate(database);
			}
		}
		else
		{
			Iterator ids = this.getInactiveDatabases().iterator();
			
			while (ids.hasNext())
			{
				String id = (String) ids.next();
				
				Database database = this.getDatabase(id);
				
				if (this.isAlive(database))
				{
					this.activate(database);
				}
			}
		}
	}
}
