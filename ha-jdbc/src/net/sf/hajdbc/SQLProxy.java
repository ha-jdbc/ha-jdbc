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
	
	private SQLProxy parentSQLObject;
	private Operation parentOperation;
	private Map sqlObjectMap;
	private List operationList = new LinkedList();
	
	protected SQLProxy(SQLProxy sqlObject, Operation operation) throws java.sql.SQLException
	{
		this(sqlObject.executeWrite(operation));
		
		this.parentOperation = operation;
		this.parentSQLObject = sqlObject;
	}
	
	protected SQLProxy(Map sqlObjectMap)
	{
		this.sqlObjectMap = sqlObjectMap;
	}
	
	public Object getSQLObject(Database database)
	{
		synchronized (this.sqlObjectMap)
		{
			Object sqlObject = this.sqlObjectMap.get(database);
			
			if ((sqlObject == null) && (this.parentSQLObject != null))
			{
				try
				{
					sqlObject = this.parentOperation.execute(database, this.parentSQLObject);
					
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
					log.warn("");
				}
			}
			
			return sqlObject;
		}
	}
	
	protected void record(Operation operation)
	{
		this.operationList.add(operation);
	}
	
	public final Object firstValue(Map valueMap)
	{
		return valueMap.values().iterator().next();
	}
	
	public final Object executeRead(Operation operation) throws java.sql.SQLException
	{
		Database database = this.getDatabaseCluster().nextDatabase();
		Object sqlObject = this.getSQLObject(database);
		
		if (sqlObject == null)
		{
			this.deactivate(database);
			
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
	
	public final Object executeGet(Operation operation) throws java.sql.SQLException
	{
		Database database = this.getDatabaseCluster().firstDatabase();
		Object sqlObject = this.getSQLObject(database);
		
		if (sqlObject == null)
		{
			this.deactivate(database);
			
			return this.executeGet(operation);
		}
		
		return operation.execute(database, sqlObject);
	}
	
	public final Map executeWrite(Operation operation) throws java.sql.SQLException
	{
		List databaseList = this.getDatabaseCluster().getActiveDatabaseList();
		Thread[] threads = new Thread[databaseList.size()];
		
		Map returnValueMap = new HashMap(threads.length);
		Map exceptionMap = new HashMap(threads.length);
		
		for (int i = 0; i < threads.length; ++i)
		{
			Database database = (Database) databaseList.get(i);
			Object sqlObject = this.getSQLObject(database);
			
			if (sqlObject == null)
			{
				this.deactivate(database);
				
				continue;
			}
			
			Executor executor = new Executor(operation, database, sqlObject, returnValueMap, exceptionMap);
			
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
		
		// If no databases returned successfully, return an exception back to the caller
		if (returnValueMap.isEmpty())
		{
			if (exceptionMap.isEmpty())
			{
				throw new SQLException("No active databases in cluster " + this.getDatabaseCluster().getName());
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

	public final Map executeSet(Operation operation) throws java.sql.SQLException
	{
		List databaseList = this.getDatabaseCluster().getActiveDatabaseList();
		Map returnValueMap = new HashMap(databaseList.size());

		for (int i = 0; i < databaseList.size(); ++i)
		{
			Database database = (Database) databaseList.get(i);
			Object sqlObject = this.getSQLObject(database);
			
			if (sqlObject == null)
			{
				this.deactivate(database);
				
				continue;
			}
			
			Object returnValue = operation.execute(database, sqlObject);
			
			returnValueMap.put(database, returnValue);
		}

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
		
		return returnValueMap;
	}
	
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
		this.deactivate(database, new SQLException("Database was not active at the time " + this.getClass().getName() + " was created"));
	}
	
	protected final void deactivate(Database database, Throwable cause)
	{
		DatabaseCluster databaseCluster = this.getDatabaseCluster();
		
		if (databaseCluster.deactivate(database))
		{
			log.error("Database " + database.getId() + " from cluster " + databaseCluster.getName() + " was deactivated.", cause);
		}
	}
	
	public abstract DatabaseCluster getDatabaseCluster();

	private class Executor implements Runnable
	{
		private Operation operation;
		private Database database;
		private Object sqlObject;
		private Map returnValueMap;
		private Map exceptionMap;
		
		public Executor(Operation operation, Database database, Object sqlObject, Map returnValueMap, Map exceptionMap)
		{
			this.operation = operation;
			this.database = database;
			this.sqlObject = sqlObject;
			this.returnValueMap = returnValueMap;
			this.exceptionMap = exceptionMap;
		}
		
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
