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

import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

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
	private static final String MBEAN_DOMAIN = "net.sf.hajdbc";
	private static final String MBEAN_KEY = "cluster";
	
	private static Log log = LogFactory.getLog(DatabaseCluster.class);
	
	/**
	 * Convenience method for constructing an mbean ObjectName for this cluster.
	 * The ObjectName is constructed using {@link #MBEAN_DOMAIN} and {@link #MBEAN_KEY} and the quoted cluster identifier.
	 * @param clusterId a cluster identifier
	 * @return an ObjectName for this cluster
	 * @throws MalformedObjectNameException if the ObjectName could not be constructed
	 */
	public static ObjectName getObjectName(String clusterId) throws MalformedObjectNameException
	{
		return ObjectName.getInstance(MBEAN_DOMAIN, MBEAN_KEY, ObjectName.quote(clusterId));
	}
	
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
		Object[] args = new Object[] { databaseId, this };
		
		try
		{
			if (this.activate(this.getDatabase(databaseId)))
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
	
	/**
	 * @see net.sf.hajdbc.DatabaseClusterMBean#activate(java.lang.String, java.lang.String)
	 */
	public final void activate(String databaseId, String strategyId) throws java.sql.SQLException
	{
		try
		{
			this.activate(this.getDatabase(databaseId), this.getSynchronizationStrategy(strategyId));
		}
		catch (java.sql.SQLException e)
		{
			log.warn(Messages.getMessage(Messages.DATABASE_ACTIVATE_FAILED, databaseId), e);
			
			throw e;
		}
	}
	
	/**
	 * Synchronizes and activates the specified database using the specified synchronization strategy
	 * @param inactiveDatabase an inactive database
	 * @param strategy a synchronization strategy
	 * @throws java.sql.SQLException if synchronization fails
	 */
	private void activate(Database inactiveDatabase, SynchronizationStrategy strategy) throws java.sql.SQLException
	{
		if (this.isActive(inactiveDatabase))
		{
			return;
		}
		
		Database[] databases = this.getDatabases();
		
		Connection inactiveConnection = null;
		Connection[] activeConnections = new Connection[databases.length];
		
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
			for (int i = 0; i < databases.length; ++i)
			{
				Database activeDatabase = databases[i];
				
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

			strategy.synchronize(inactiveConnection, activeConnections[0], tableList);
			
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
	 * @see java.lang.Object#toString()
	 */
	public String toString()
	{
		return this.getId();
	}
	
	/**
	 * Determines whether the specified database is active.
	 * @param database a database descriptor
	 * @return true, if the database is active, false otherwise
	 */
	public abstract boolean isActive(Database database);

	/**
	 * Activates the specified database
	 * @param database a database descriptor
	 * @return true, if the database was activated, false it was already active
	 */
	public abstract boolean activate(Database database);
	
	/**
	 * Deactivates the specified database
	 * @param database a database descriptor
	 * @return true, if the database was deactivated, false it was already inactive
	 */
	public abstract boolean deactivate(Database database);
	
	/**
	 * Returns the first database in this cluster ignoring load balancing strategy.
	 * @return a database descriptor
	 * @throws java.sql.SQLException if the cluster is empty
	 */
	public abstract Database firstDatabase() throws java.sql.SQLException;
	
	/**
	 * Returns the next available database in this cluster determined via the load balancing strategy.
	 * @return a database descriptor
	 * @throws java.sql.SQLException if the cluster is empty
	 */
	public abstract Database nextDatabase() throws java.sql.SQLException;

	/**
	 * Returns all the databases in this cluster.
	 * @return an array of Database descriptors
	 * @throws java.sql.SQLException if the cluster is empty
	 */
	public abstract Database[] getDatabases() throws java.sql.SQLException;

	/**
	 * Returns a connection factory proxy for this obtaining connections to databases in this cluster.
	 * @return a connection factory proxy
	 */
	public abstract ConnectionFactoryProxy getConnectionFactory();
	
	/**
	 * Determines whether or not the specified database is responding
	 * @param database a database descriptor
	 * @return true, if the database is responding, false if it appears down
	 */
	public abstract boolean isAlive(Database database);
	
	/**
	 * Returns the database identified by the specified id
	 * @param id a database identifier
	 * @return a database descriptor
	 * @throws java.sql.SQLException if no database exists with the specified identifier
	 */
	public abstract Database getDatabase(String id) throws java.sql.SQLException;
	
	/**
	 * Returns the synchronization strategy identified by the specified id
	 * @param id synchronization strategy unique identifier
	 * @return a SynchronizationStrategy implementation
	 * @throws java.sql.SQLException if no strategy exists with the specified identifier
	 */
	public abstract SynchronizationStrategy getSynchronizationStrategy(String id) throws java.sql.SQLException;
	
	public abstract void init() throws java.sql.SQLException;
}
