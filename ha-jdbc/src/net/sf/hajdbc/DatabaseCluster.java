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
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Contains a map of <code>Database</code> -&gt; database connection factory (i.e. Driver, DataSource, ConnectionPoolDataSource, XADataSource)
 * @author  Paul Ferraro
 * @version $Revision$
 * @since   1.0
 */
public abstract class DatabaseCluster implements DatabaseClusterMBean
{
	private static Log log = LogFactory.getLog(DatabaseCluster.class);
	
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
		try
		{
			this.deactivate(this.getDatabase(databaseId));
		}
		catch (java.sql.SQLException e)
		{
			log.warn(Messages.getMessage(Messages.DATABASE_DEACTIVATE_FAILED, databaseId), e);
			
			throw e;
		}
	}

	/**
	 * @see net.sf.hajdbc.DatabaseClusterMBean#activate(java.lang.String)
	 */
	public final void activate(String databaseId) throws java.sql.SQLException
	{
		this.activate(databaseId, this.getDescriptor().getDefaultSynchronizationStrategy());
	}
	
	/**
	 * @see net.sf.hajdbc.DatabaseClusterMBean#activate(java.lang.String, java.lang.String)
	 */
	public final void activate(String databaseId, String strategyClassName) throws java.sql.SQLException
	{
		try
		{
			Database database = this.getDatabase(databaseId);
			
			// If this database is already active, or there are no active databases then skip synchronization
			if (this.isActive(database) || this.getActiveDatabaseList().isEmpty())
			{
				this.activate(database);
			}
			else
			{
				try
				{
					Class strategyClass = Class.forName(strategyClassName);
					
					if (!SynchronizationStrategy.class.isAssignableFrom(strategyClass))
					{
						throw new SQLException(Messages.getMessage(Messages.INVALID_SYNC_STRATEGY, SynchronizationStrategy.class.getName()));
					}
					
					SynchronizationStrategy strategy = (SynchronizationStrategy) strategyClass.newInstance();
	
					this.activate(database, strategy);
				}
				catch (ClassNotFoundException e)
				{
					throw new SQLException(e);
				}
				catch (InstantiationException e)
				{
					throw new SQLException(e);
				}
				catch (IllegalAccessException e)
				{
					throw new SQLException(e);
				}
			}
		}
		catch (java.sql.SQLException e)
		{
			log.warn(Messages.getMessage(Messages.DATABASE_ACTIVATE_FAILED, databaseId), e);
			
			throw e;
		}
	}
	
	public final void activate(Database inactiveDatabase, SynchronizationStrategy strategy) throws java.sql.SQLException
	{
		Connection inactiveConnection = null;
		List databaseList = this.getActiveDatabaseList();
		Connection[] activeConnections = new Connection[databaseList.size()];
		
		try
		{
			inactiveConnection = inactiveDatabase.connect(this.getConnectionFactory().getSQLObject(inactiveDatabase));
			
			List tableList = new LinkedList();
			
			DatabaseMetaData databaseMetaData = inactiveConnection.getMetaData();
			ResultSet resultSet = databaseMetaData.getTables(null, null, "%", new String[] { "TABLE" });
			
			while (resultSet.next())
			{
				String table = resultSet.getString("TABLE_NAME");
				tableList.add(table);
			}
			
			resultSet.close();

			// Open connections to all active databases
			for (int i = 0; i < databaseList.size(); ++i)
			{
				Database activeDatabase = (Database) databaseList.get(i);
				
				activeConnections[i] = activeDatabase.connect(DatabaseCluster.this.getConnectionFactory().getSQLObject(activeDatabase));
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
					
					statement.execute("SELECT count(*) FROM " + table);
				}
				
				statement.close();
			}
			
			log.info(Messages.getMessage(Messages.DATABASE_SYNC_START, inactiveDatabase));

			strategy.synchronize(inactiveConnection, activeConnections[0], tableList, this.getDescriptor());
			
			log.info(Messages.getMessage(Messages.DATABASE_SYNC_END, inactiveDatabase));
	
			this.activate(inactiveDatabase);
			
			// Release table locks
			for (int i = 0; i < activeConnections.length; ++i)
			{
				activeConnections[i].rollback();
			}
		}
		finally
		{
			this.close(inactiveConnection, inactiveDatabase);
			
			for (int i = 0; i < activeConnections.length; ++i)
			{
				this.close(activeConnections[i], (Database) databaseList.get(i));
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
	
	public String toString()
	{
		return this.getId();
	}
	
	public abstract boolean isActive(Database database);

	public abstract boolean activate(Database database);
	
	public abstract boolean addDatabase(Database database);
	
	public abstract boolean removeDatabase(Database database);
	
	public abstract Database firstDatabase() throws java.sql.SQLException;
	
	public abstract Database nextDatabase() throws java.sql.SQLException;

	public abstract List getActiveDatabaseList() throws java.sql.SQLException;

	public abstract Collection getActiveDatabases() throws java.sql.SQLException;

	public abstract Collection getInactiveDatabases() throws java.sql.SQLException;
	
	public abstract ConnectionFactoryProxy getConnectionFactory();
	
	public abstract boolean isAlive(Database database);
	
	public abstract boolean deactivate(Database database);

	public abstract DatabaseClusterDescriptor getDescriptor();
	
	public abstract Database getDatabase(String databaseId) throws java.sql.SQLException;
	
	public abstract Set getNewDatabaseSet(Collection databases);
}
