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
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

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
			inactiveConnection = inactiveDatabase.connect(this.getConnectionFactory().getObject(inactiveDatabase));
			
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
				
				activeConnections[i] = activeDatabase.connect(DatabaseCluster.this.getConnectionFactory().getObject(activeDatabase));
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
	 * Returns all the databases in this cluster.
	 * @return an array of Database descriptors
	 * @throws java.sql.SQLException if the cluster is empty
	 */
	public abstract Database[] getDatabases() throws java.sql.SQLException;

	/**
	 * Returns a connection factory proxy for this obtaining connections to databases in this cluster.
	 * @return a connection factory proxy
	 */
	public ConnectionFactoryProxy getConnectionFactory()
	{
		return new ConnectionFactoryProxy(this, this.getConnectionFactoryMap());
	}
	
	protected abstract Map getConnectionFactoryMap();
	
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
	
	/**
	 * Initializes this database cluster.
	 * @throws java.sql.SQLException if initialization fails
	 */
	public abstract void init() throws java.sql.SQLException;
	
	protected abstract Balancer getBalancer();
	
	/**
	 * Read-style execution that executes the specified operation on a single database in the cluster.
	 * It is assumed that these types of operation will require access to the database.
	 * @param proxy a sql object proxy
	 * @param operation a database operation
	 * @return the operation execution result
	 * @throws java.sql.SQLException if operation execution fails
	 */
	public Object executeReadFromDatabase(SQLProxy proxy, Operation operation) throws java.sql.SQLException
	{
		while (true)
		{
			Database database = this.nextDatabase();
			
			Object object = proxy.getObject(database);
			
			if (object != null)
			{
				try
				{
					return this.getBalancer().execute(operation, database, object);
				}
				catch (Throwable e)
				{
					this.handleFailure(database, e);
				}
			}
		}
	}
	
	/**
	 * Get-style execution that executes the specified operation on a single database in the cluster.
	 * It is assumed that these types of operation will <em>not</em> require access to the database.
	 * @param proxy a sql object proxy
	 * @param operation a database operation
	 * @return the operation execution result
	 * @throws java.sql.SQLException if operation execution fails
	 */
	public final Object executeReadFromDriver(SQLProxy proxy, Operation operation) throws java.sql.SQLException
	{
		Database database = null;
		Object object = null;
		
		while (object == null)
		{
			database = this.firstDatabase();
			
			object = proxy.getObject(database);
		}
		
		return operation.execute(database, object);
	}
	
	/**
	 * Write-style execution that executes the specified operation on every database in the cluster in parallel.
	 * It is assumed that these types of operation will require access to the database.
	 * @param proxy a sql object proxy
	 * @param operation a database operation
	 * @return a Map<Database, Object> of operation execution results from each database
	 * @throws java.sql.SQLException if operation execution fails
	 */
	public final Map executeWriteToDatabase(SQLProxy proxy, Operation operation) throws java.sql.SQLException
	{
		Database[] databases = this.getDatabases();
		Thread[] threads = new Thread[databases.length];
		
		Map returnValueMap = new HashMap(databases.length);
		Map exceptionMap = new HashMap(databases.length);

		for (int i = 0; i < databases.length; ++i)
		{
			Database database = databases[i];
			Object object = proxy.getObject(database);
			
			if (object != null)
			{
				threads[i] = new Thread(new OperationExecutor(this, operation, database, object, returnValueMap, exceptionMap));
				threads[i].start();
			}
		}
		
		// Wait until all threads have completed
		for (int i = 0; i < threads.length; ++i)
		{
			Thread thread = threads[i];
			
			if ((thread != null) && thread.isAlive())
			{
				try
				{
					thread.join();
				}
				catch (InterruptedException e)
				{
					// Ignore
				}
			}
		}
		
		this.deactivateNewDatabases(databases);
		
		// If no databases returned successfully, return an exception back to the caller
		if (returnValueMap.isEmpty())
		{
			if (exceptionMap.isEmpty())
			{
				throw new SQLException(Messages.getMessage(Messages.NO_ACTIVE_DATABASES, this));
			}
			
			throw new SQLException((Throwable) exceptionMap.get(databases[0]));
		}
		
		// If any databases failed, while others succeeded, deactivate them
		if (!exceptionMap.isEmpty())
		{
			Iterator exceptionMapEntries = exceptionMap.entrySet().iterator();
			
			while (exceptionMapEntries.hasNext())
			{
				Map.Entry exceptionMapEntry = (Map.Entry) exceptionMapEntries.next();
				Database database = (Database) exceptionMapEntry.getKey();
				Throwable exception = (Throwable) exceptionMapEntry.getValue();
				
				this.deactivate(database, exception);
			}
		}
		
		// Return results from successful operations
		return returnValueMap;
	}
	
	/**
	 * Set-style execution that executes the specified operation on every database in the cluster.
	 * It is assumed that these types of operation will <em>not</em> require access to the database.
	 * @param proxy a sql object proxy
	 * @param operation a database operation
	 * @return a Map<Database, Object> of operation execution results from each database
	 * @throws java.sql.SQLException if operation execution fails
	 */
	public final Map executeWriteToDriver(SQLProxy proxy, Operation operation) throws java.sql.SQLException
	{
		Database[] databases = this.getBalancer().toArray();
		
		if (databases.length == 0)
		{
			throw new SQLException(Messages.getMessage(Messages.NO_ACTIVE_DATABASES, this));
		}
		
		Map returnValueMap = new HashMap(databases.length);

		for (int i = 0; i < databases.length; ++i)
		{
			Database database = databases[i];
			Object object = proxy.getObject(database);
			
			if (object != null)
			{
				Object returnValue = operation.execute(database, object);
				
				returnValueMap.put(database, returnValue);
			}
		}
		
		this.deactivateNewDatabases(databases);
		
		proxy.record(operation);
		
		return returnValueMap;
	}
	
	/**
	 * Returns the first database from the balancer
	 * @return a database descriptor
	 * @throws SQLException if there are no active databases in the cluster
	 */
	public Database firstDatabase() throws SQLException
	{
		try
		{
			return this.getBalancer().first();
		}
		catch (NoSuchElementException e)
		{
			throw new SQLException(Messages.getMessage(Messages.NO_ACTIVE_DATABASES, this));
		}
	}
	
	/**
	 * Returns the next database from the balancer
	 * @return a database descriptor
	 * @throws SQLException if there are no active databases in the cluster
	 */
	public Database nextDatabase() throws SQLException
	{
		try
		{
			return this.getBalancer().next();
		}
		catch (NoSuchElementException e)
		{
			throw new SQLException(Messages.getMessage(Messages.NO_ACTIVE_DATABASES, this));
		}
	}
	
	/**
	 * Handles a failure caused by the specified cause on the specified database.
	 * If the database is not alive, then it is deactivated, otherwise an exception is thrown back to the caller.
	 * @param database a database descriptor
	 * @param cause the cause of the failure
	 * @throws SQLException if the database is alive
	 */
	public final void handleFailure(Database database, Throwable cause) throws SQLException
	{
		if (this.isAlive(database))
		{
			throw new SQLException(cause);
		}
		
		this.deactivate(database, cause);
	}
	
	private void deactivate(Database database, Throwable cause)
	{
		if (this.deactivate(database))
		{
			log.error(Messages.getMessage(Messages.DATABASE_DEACTIVATED, new Object[] { database, this }), cause);
		}
	}

	private void deactivateNewDatabases(Database[] databases)
	{
		Set databaseSet = new HashSet(Arrays.asList(this.getBalancer().toArray()));
		
		for (int i = 0; i < databases.length; ++i)
		{
			databaseSet.remove(databases[i]);
		}
		
		if (!databaseSet.isEmpty())
		{
			Iterator newDatabases = databaseSet.iterator();
			
			while (newDatabases.hasNext())
			{
				Database newDatabase = (Database) newDatabases.next();
				
				this.deactivate(newDatabase);
			}
		}
	}
}
