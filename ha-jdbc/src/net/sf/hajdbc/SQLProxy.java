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

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * @author  Paul Ferraro
 * @version $Revision$
 * @since   1.0
 */
public abstract class SQLProxy
{
	private static Log log = LogFactory.getLog(SQLProxy.class);
	
	protected SQLProxy parent;
	private Operation parentOperation;
	private Map sqlObjectMap;
	private List operationList = new LinkedList();
	
	protected SQLProxy(SQLProxy sqlObject, Operation operation) throws java.sql.SQLException
	{
		this(sqlObject, sqlObject.executeWrite(operation));
		
		this.parentOperation = operation;
	}
	
	protected SQLProxy(SQLProxy sqlObject, Map sqlObjectMap)
	{
		this(sqlObjectMap);
		
		this.parent = sqlObject;
	}
	
	protected SQLProxy(Map sqlObjectMap)
	{
		this.sqlObjectMap = sqlObjectMap;
	}
	
	/**
	 * Returns the underlying SQL object for the specified database.
	 * If the sql object does not exist (this might be the case if the database was newly activated), it will be created from the stored operation.
	 * Any recorded operations are also executed. If the object could not be created, or if any of the executed operations failed, then the specified database is deactivated.
	 * @param database a database descriptor.
	 * @return an underlying SQL object
	 */
	public Object getSQLObject(Database database)
	{
		synchronized (this.sqlObjectMap)
		{
			Object sqlObject = this.sqlObjectMap.get(database);
			
			if (sqlObject == null)
			{
				try
				{
					Object parentObject = this.parent.getSQLObject(database);
					
					if (parentObject == null)
					{
						throw new SQLException(Messages.getMessage(Messages.SQL_OBJECT_INIT_FAILED, new Object[] { this.parent.getClass().getName(), database }));
					}
					
					sqlObject = this.parentOperation.execute(database, parentObject);
					
					Iterator operations = this.operationList.iterator();
					
					while (operations.hasNext())
					{
						Operation operation = (Operation) operations.next();
						
						operation.execute(database, sqlObject);
					}
					
					this.sqlObjectMap.put(database, sqlObject);
				}
				catch (java.sql.SQLException e)
				{
					String message = Messages.getMessage(Messages.SQL_OBJECT_INIT_FAILED, new Object[] { this.getClass().getName(), database });
					
					this.deactivate(database, new SQLException(message, e));
				}
			}
			
			return sqlObject;
		}
	}
	
	/**
	 * Records an operation.
	 * @param operation a database operation
	 */
	protected void record(Operation operation)
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
	 * Read-style execution that executes the specified operation on a single database in the cluster.
	 * It is assumed that these types of operation will require access to the database.
	 * @param operation a database operation
	 * @return the operation execution result
	 * @throws java.sql.SQLException if operation execution fails
	 */
	public final Object executeRead(Operation operation) throws java.sql.SQLException
	{
		Database database = this.getDatabaseCluster().nextDatabase();
		Object sqlObject = this.getSQLObject(database);
		
		if (sqlObject == null)
		{
			return this.executeRead(operation);
		}
		
		try
		{
			return operation.execute(database, sqlObject);
		}
		catch (java.sql.SQLException e)
		{
			this.handleException(database, e);
			
			// Retry with next database in cluster...
			return this.executeRead(operation);
		}
	}
	
	/**
	 * Get-style execution that executes the specified operation on a single database in the cluster.
	 * It is assumed that these types of operation will <em>not</em> require access to the database.
	 * @param operation a database operation
	 * @return the operation execution result
	 * @throws java.sql.SQLException if operation execution fails
	 */
	public final Object executeGet(Operation operation) throws java.sql.SQLException
	{
		Database database = this.getDatabaseCluster().firstDatabase();
		Object sqlObject = this.getSQLObject(database);
		
		if (sqlObject == null)
		{
			return this.executeGet(operation);
		}
		
		return operation.execute(database, sqlObject);
	}
	
	/**
	 * Write-style execution that executes the specified operation on every database in the cluster in parallel.
	 * It is assumed that these types of operation will require access to the database.
	 * @param operation a database operation
	 * @return a Map<Database, Object> of operation execution results from each database
	 * @throws java.sql.SQLException if operation execution fails
	 */
	public final Map executeWrite(Operation operation) throws java.sql.SQLException
	{
		List databaseList = this.getDatabaseCluster().getDatabaseList();
		Thread[] threads = new Thread[databaseList.size()];
		
		Map returnValueMap = new HashMap(threads.length);
		Map exceptionMap = new HashMap(threads.length);
		
		for (int i = 0; i < threads.length; ++i)
		{
			Database database = (Database) databaseList.get(i);
			Object sqlObject = this.getSQLObject(database);
			
			if (sqlObject == null)
			{
				continue;
			}
			
			OperationExecutor executor = new OperationExecutor(operation, database, sqlObject, returnValueMap, exceptionMap);
			
			threads[i] = new Thread(executor);
			threads[i].start();
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

		this.deactivateNewDatabases(databaseList);
		
		// If no databases returned successfully, return an exception back to the caller
		if (returnValueMap.isEmpty())
		{
			if (exceptionMap.isEmpty())
			{
				throw new SQLException(Messages.getMessage(Messages.NO_ACTIVE_DATABASES, this.getDatabaseCluster()));
			}
			
			throw new SQLException((Throwable) exceptionMap.get(databaseList.get(0)));
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
	 * @param operation a database operation
	 * @return a Map<Database, Object> of operation execution results from each database
	 * @throws java.sql.SQLException if operation execution fails
	 */
	public final Map executeSet(Operation operation) throws java.sql.SQLException
	{
		List databaseList = this.getDatabaseCluster().getDatabaseList();
		Map returnValueMap = new HashMap(databaseList.size());

		for (int i = 0; i < databaseList.size(); ++i)
		{
			Database database = (Database) databaseList.get(i);
			Object sqlObject = this.getSQLObject(database);
			
			if (sqlObject == null)
			{
				continue;
			}
			
			Object returnValue = operation.execute(database, sqlObject);
			
			returnValueMap.put(database, returnValue);
		}

		this.deactivateNewDatabases(databaseList);
		
		this.record(operation);
		
		return returnValueMap;
	}
	
	private void deactivateNewDatabases(List databaseList)
	{
		Set databaseSet = this.getDatabaseCluster().getNewDatabaseSet(databaseList);
		
		if (!databaseSet.isEmpty())
		{
			Iterator databases = databaseSet.iterator();
			
			while (databases.hasNext())
			{
				Database database = (Database) databases.next();
				
				this.deactivate(database);
			}
		}
	}
	
	/**
	 * @param database
	 * @param exception
	 * @throws SQLException
	 */
	protected final void handleException(Database database, Throwable exception) throws SQLException
	{
		if (this.getDatabaseCluster().isAlive(database))
		{
			throw new SQLException(exception);
		}
		
		this.deactivate(database, exception);
	}

	private void deactivate(Database database)
	{
		this.deactivate(database, new SQLException(Messages.getMessage(Messages.DATABASE_NOT_ACTIVE, this.getClass().getName())));
	}
	
	/**
	 * @param database
	 * @param cause
	 */
	protected final void deactivate(Database database, Throwable cause)
	{
		DatabaseCluster databaseCluster = this.getDatabaseCluster();
		
		if (databaseCluster.deactivate(database))
		{
			log.error(Messages.getMessage(Messages.DATABASE_DEACTIVATED, new Object[] { database, databaseCluster }), cause);
		}
	}
	
	/**
	 * Returns the database cluster for this SQL object.
	 * @return a database cluster
	 */
	public DatabaseCluster getDatabaseCluster()
	{
		return this.parent.getDatabaseCluster();
	}

	/**
	 * Helper class that enables asynchronous execution of an operation.
	 */
	private class OperationExecutor implements Runnable
	{
		private Operation operation;
		private Database database;
		private Object sqlObject;
		private Map returnValueMap;
		private Map exceptionMap;
		
		/**
		 * Constructs a new OperationExecutor.
		 * @param operation a database operation
		 * @param database a database descriptor
		 * @param sqlObject a SQL object
		 * @param returnValueMap a Map<Database, Object> that holds the results from the operation execution
		 * @param exceptionMap a Map<Database, SQLException> that holds the exceptions resulting from the operation execution
		 */
		public OperationExecutor(Operation operation, Database database, Object sqlObject, Map returnValueMap, Map exceptionMap)
		{
			this.operation = operation;
			this.database = database;
			this.sqlObject = sqlObject;
			this.returnValueMap = returnValueMap;
			this.exceptionMap = exceptionMap;
		}
		
		/**
		 * @see java.lang.Runnable#run()
		 */
		public void run()
		{
			try
			{
				Object returnValue = this.operation.execute(this.database, this.sqlObject);
				
				synchronized (this.returnValueMap)
				{
					this.returnValueMap.put(this.database, returnValue);
				}
			}
			catch (Throwable e)
			{
				try
				{
					SQLProxy.this.handleException(this.database, e);
				}
				catch (Throwable exception)
				{
					synchronized (this.exceptionMap)
					{
						this.exceptionMap.put(this.database, e);
					}
				}
			}
		}
	}
}
