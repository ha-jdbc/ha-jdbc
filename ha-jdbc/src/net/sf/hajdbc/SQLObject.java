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

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import edu.emory.mathcs.backport.java.util.concurrent.Callable;
import edu.emory.mathcs.backport.java.util.concurrent.ExecutionException;
import edu.emory.mathcs.backport.java.util.concurrent.ExecutorService;
import edu.emory.mathcs.backport.java.util.concurrent.Future;

/**
 * @author  Paul Ferraro
 * @since   1.0
 */
public class SQLObject
{
	private static Log log = LogFactory.getLog(SQLObject.class);
	
	protected SQLObject parent;
	private DatabaseCluster databaseCluster;
	private Operation parentOperation;
	private Map objectMap;
	private List operationList = new LinkedList();
	
	protected SQLObject(SQLObject parent, Operation operation) throws java.sql.SQLException
	{
		this(parent, operation, null);
	}
	
	protected SQLObject(SQLObject parent, Operation operation, String object) throws java.sql.SQLException
	{
		this(parent.getDatabaseCluster(), parent.executeWriteToDatabase(operation, object));
		
		this.parent = parent;
		this.parentOperation = operation;
	}
	
	protected SQLObject(DatabaseCluster databaseCluster, Map objectMap)
	{
		this.databaseCluster = databaseCluster;
		this.objectMap = objectMap;
	}
	
	/**
	 * Returns the underlying SQL object for the specified database.
	 * If the sql object does not exist (this might be the case if the database was newly activated), it will be created from the stored operation.
	 * Any recorded operations are also executed. If the object could not be created, or if any of the executed operations failed, then the specified database is deactivated.
	 * @param database a database descriptor.
	 * @return an underlying SQL object
	 */
	public synchronized final Object getObject(Database database)
	{
		Object object = this.objectMap.get(database);
		
		if (object == null)
		{
			try
			{
				if (this.parent == null)
				{
					throw new java.sql.SQLException();
				}
				
				Object parentObject = this.parent.getObject(database);
				
				if (parentObject == null)
				{
					throw new java.sql.SQLException();
				}
				
				object = this.parentOperation.execute(database, parentObject);
				
				Iterator operations = this.operationList.iterator();
				
				while (operations.hasNext())
				{
					Operation operation = (Operation) operations.next();
					
					operation.execute(database, object);
				}
				
				this.objectMap.put(database, object);
			}
			catch (java.sql.SQLException e)
			{
				log.warn(Messages.getMessage(Messages.SQL_OBJECT_INIT_FAILED, new Object[] { this.getClass().getName(), database }), e);
				
				this.databaseCluster.deactivate(database);
			}
		}
		
		return object;
	}
	
	/**
	 * Records an operation.
	 * @param operation a database operation
	 */
	protected synchronized final void record(Operation operation)
	{
		this.operationList.add(operation);
	}
	
	/**
	 * Helper method that extracts the first result from a map of results.
	 * @param valueMap a Map<Database, Object> of operation execution results.
	 * @return a operation execution result
	 */
	protected final Object firstValue(Map valueMap)
	{
		return valueMap.values().iterator().next();
	}

	/**
	 * Executes the specified read operation on a single database in the cluster.
	 * It is assumed that these types of operation will <em>not</em> require access to the database.
	 * @param operation a database operation
	 * @return the result of the operation
	 * @throws java.sql.SQLException if operation execution fails
	 */
	public final Object executeReadFromDriver(Operation operation) throws java.sql.SQLException
	{
		try
		{
			Database database = this.databaseCluster.getBalancer().first();
			Object object = this.getObject(database);
			
			return operation.execute(database, object);
		}
		catch (NoSuchElementException e)
		{
			throw new SQLException(Messages.getMessage(Messages.NO_ACTIVE_DATABASES, this.databaseCluster));
		}
	}

	/**
	 * Executes the specified read operation on a single database in the cluster.
	 * It is assumed that these types of operation will require access to the database.
	 * @param operation a database operation
	 * @return the result of the operation
	 * @throws java.sql.SQLException if operation execution fails
	 */
	public final Object executeReadFromDatabase(Operation operation) throws java.sql.SQLException
	{
		Balancer balancer = this.databaseCluster.getBalancer();
		
		try
		{
			while (true)
			{
				Database database = balancer.next();
				Object object = this.getObject(database);
	
				try
				{
					balancer.beforeOperation(database);
					
					return operation.execute(database, object);
				}
				catch (java.sql.SQLException e)
				{
					this.databaseCluster.handleFailure(database, e);
				}
				finally
				{
					balancer.afterOperation(database);
				}
			}
		}
		catch (NoSuchElementException e)
		{
			throw new SQLException(Messages.getMessage(Messages.NO_ACTIVE_DATABASES, this.databaseCluster));
		}
	}

	/**
	 * Executes the specified write operation on every database in the cluster in parallel.
	 * It is assumed that these types of operation will require access to the database.
	 * @param operation a database operation
	 * @return the result of the operation
	 * @throws java.sql.SQLException if operation execution fails
	 */
	public final Map executeWriteToDatabase(final Operation operation) throws java.sql.SQLException
	{
		ExecutorService executor = this.databaseCluster.getExecutor();
		Database[] databases = this.getDatabases();
		Map futureMap = new HashMap();
		
		for (int i = 0; i < databases.length; ++i)
		{
			final Database database = databases[i];
			final Object object = this.getObject(database);
			
			Callable callable = new Callable()
			{
				public Object call() throws java.sql.SQLException
				{
					return operation.execute(database, object);
				}
			};

			futureMap.put(database, executor.submit(callable));
		}

		Map returnValueMap = new HashMap();
		Map exceptionMap = new HashMap();
		
		for (int i = 0; i < databases.length; ++i)
		{
			Database database = databases[i];
			
			Future future = (Future) futureMap.get(databases[i]);
			
			try
			{
				returnValueMap.put(database, future.get());
			}
			catch (ExecutionException e)
			{
				SQLException cause = new SQLException(e.getCause());

				try
				{
					this.databaseCluster.handleFailure(database, cause);
				}
				catch (java.sql.SQLException sqle)
				{
					exceptionMap.put(database, sqle);
				}
			}
			catch (InterruptedException e)
			{
				throw new SQLException(e);
			}
		}
		
		this.deactivateNewDatabases(databases);
		
		// If no databases returned successfully, return an exception back to the caller
		if (returnValueMap.isEmpty())
		{
			if (exceptionMap.isEmpty())
			{
				throw new SQLException(Messages.getMessage(Messages.NO_ACTIVE_DATABASES, this.databaseCluster));
			}
			
			throw (java.sql.SQLException) exceptionMap.values().iterator().next();
		}
		
		// If any databases failed, while others succeeded, deactivate them
		if (!exceptionMap.isEmpty())
		{
			this.handleExceptions(exceptionMap);
		}
		
		// Return results from successful operations
		return returnValueMap;
	}

	/**
	 * Acquires a lock on the specified object, then executes the specified write operation on every database in the cluster in parallel.
	 * It is assumed that these types of operation will require access to the database.
	 * @param operation a database operation
	 * @param objectSet a set of database object names
	 * @return the result of the operation
	 * @throws java.sql.SQLException if operation execution fails
	 * @since 1.1
	 */
	protected final Map executeWriteToDatabase(final Operation operation, final Set objectSet) throws java.sql.SQLException
	{
		try
		{
			if (objectSet != null)
			{
				Iterator objects = objectSet.iterator();
				
				while (objects.hasNext())
				{
					this.databaseCluster.acquireLock((String) objects.next());
				}
			}
			
			return this.executeWriteToDatabase(operation);
		}
		finally
		{
			if (objectSet != null)
			{
				Iterator objects = objectSet.iterator();
				
				while (objects.hasNext())
				{
					this.databaseCluster.releaseLock((String) objects.next());
				}
			}
		}
	}

	/**
	 * Acquires a lock on the specified object, then executes the specified write operation on every database in the cluster in parallel.
	 * It is assumed that these types of operation will require access to the database.
	 * @param operation a database operation
	 * @param object a database object name
	 * @return the result of the operation
	 * @throws java.sql.SQLException if operation execution fails
	 * @since 1.1
	 */
	protected final Map executeWriteToDatabase(final Operation operation, final String object) throws java.sql.SQLException
	{
		return this.executeWriteToDatabase(operation, (object != null) ? Collections.singleton(object) : Collections.EMPTY_SET);
	}
	
	/**
	 * Executes the specified write operation on every database in the cluster.
	 * It is assumed that these types of operation will <em>not</em> require access to the database.
	 * @param operation a database operation
	 * @return the result of the operation
	 * @throws java.sql.SQLException if operation execution fails
	 */
	public final Map executeWriteToDriver(Operation operation) throws java.sql.SQLException
	{
		Database[] databases = this.databaseCluster.getBalancer().toArray();
		
		if (databases.length == 0)
		{
			throw new SQLException(Messages.getMessage(Messages.NO_ACTIVE_DATABASES, this.databaseCluster));
		}
		
		Map returnValueMap = new HashMap();

		for (int i = 0; i < databases.length; ++i)
		{
			Database database = databases[i];
			Object object = this.getObject(database);
			
			returnValueMap.put(database, operation.execute(database, object));
		}
		
		this.deactivateNewDatabases(databases);
		
		this.record(operation);
		
		return returnValueMap;
	}
	
	/**
	 * Returns the database cluster to which this proxy is associated.
	 * @return a database cluster
	 */
	public DatabaseCluster getDatabaseCluster()
	{
		return this.databaseCluster;
	}
	
	/**
	 * @param exceptionMap
	 * @throws java.sql.SQLException
	 */
	public void handleExceptions(Map exceptionMap) throws java.sql.SQLException
	{
		Iterator exceptionMapEntries = exceptionMap.entrySet().iterator();
		
		while (exceptionMapEntries.hasNext())
		{
			Map.Entry exceptionMapEntry = (Map.Entry) exceptionMapEntries.next();
			Database database = (Database) exceptionMapEntry.getKey();
			java.sql.SQLException exception = (java.sql.SQLException) exceptionMapEntry.getValue();
			
			if (this.databaseCluster.deactivate(database))
			{
				log.error(Messages.getMessage(Messages.DATABASE_DEACTIVATED, new Object[] { database, this.databaseCluster }), exception);
			}
		}
	}

	private void deactivateNewDatabases(Database[] databases)
	{
		Set databaseSet = new HashSet(Arrays.asList(this.databaseCluster.getBalancer().toArray()));
		
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
				
				if (this.databaseCluster.deactivate(newDatabase))
				{
					log.error(Messages.getMessage(Messages.DATABASE_DEACTIVATED, new Object[] { newDatabase, this.databaseCluster }));
				}
			}
		}
	}
	
	private Database[] getDatabases() throws SQLException
	{
		Database[] databases = this.databaseCluster.getBalancer().toArray();
		
		if (databases.length == 0)
		{
			throw new SQLException(Messages.getMessage(Messages.NO_ACTIVE_DATABASES, this.databaseCluster));
		}
		
		return databases;
	}
}
