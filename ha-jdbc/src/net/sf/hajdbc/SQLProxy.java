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

import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
	
	private Map sqlObjectMap;
	
	protected SQLProxy(Map sqlObjectMap)
	{
		this.sqlObjectMap = sqlObjectMap;
	}
	
	public Object getSQLObject(Database database)
	{
		return this.sqlObjectMap.get(database);
	}
	
	public final Object executeRead(Operation operation) throws SQLException
	{
		Database database = this.getDatabaseCluster().getDescriptor().nextDatabase();
		Object sqlObject = this.sqlObjectMap.get(database);
		
		try
		{
			return operation.execute(database, sqlObject);
		}
		catch (SQLException e)
		{
			this.handleSQLException(e, database);
			
			// Retry with next database in cluster...
			return this.executeRead(operation);
		}
	}
	
	public final Object executeGet(Operation operation) throws SQLException
	{
		Database database = this.getDatabaseCluster().getDescriptor().firstDatabase();
		Object sqlObject = this.sqlObjectMap.get(database);
		
		return operation.execute(database, sqlObject);
	}
	
	public final Object firstItem(Map returnValueMap)
	{
		return returnValueMap.values().iterator().next();
	}
	
	public final Map executeWrite(Operation operation) throws SQLException
	{
		List databaseList = this.getDatabaseCluster().getDescriptor().getActiveDatabaseList();
		Thread[] threads = new Thread[databaseList.size()];
		
		Map returnValueMap = new HashMap(threads.length);
		SQLException exception = new SQLException();

		for (int i = 0; i < threads.length; ++i)
		{
			Database database = (Database) databaseList.get(i);
			Object sqlObject = this.sqlObjectMap.get(database);
			
			Executor executor = new Executor(operation, database, sqlObject, returnValueMap, exception);
			
			threads[i] = new Thread(executor);
			threads[i].start();
		}
		
		// Wait until all threads have completed
		for (int i = 0; i < threads.length; ++i)
		{
			Thread thread = threads[i];
			
			if (thread.isAlive())
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

		if (exception.getCause() != null)
		{
			throw exception;
		}
		
		return returnValueMap;
	}

	public final Map executeSet(Operation operation) throws SQLException
	{
		List databaseList = this.getDatabaseCluster().getDescriptor().getActiveDatabaseList();
		Map returnValueMap = new HashMap(databaseList.size());

		for (int i = 0; i < databaseList.size(); ++i)
		{
			Database database = (Database) databaseList.get(i);
			Object sqlObject = this.sqlObjectMap.get(database);
			
			Object returnValue = operation.execute(database, sqlObject);
			
			returnValueMap.put(database, returnValue);
		}
		
		return returnValueMap;
	}
	
	protected final void handleSQLException(SQLException exception, Database database) throws SQLException
	{
		DatabaseCluster databaseCluster = this.getDatabaseCluster();
		
		if (databaseCluster.isActive(database))
		{
			throw exception;
		}
		
		if (databaseCluster.deactivate(database))
		{
			log.error("Database " + database.getId() + " from cluster " + this.getDatabaseCluster().getDescriptor().getName() + " was deactivated.", exception);
		}
	}
	
	protected abstract DatabaseCluster getDatabaseCluster();

	private class Executor implements Runnable
	{
		private Operation operation;
		private Database database;
		private Object sqlObject;
		private Map returnValueMap;
		private SQLException exception;
		
		public Executor(Operation operation, Database database, Object sqlObject, Map returnValueMap, SQLException exception)
		{
			this.operation = operation;
			this.database = database;
			this.sqlObject = sqlObject;
			this.returnValueMap = returnValueMap;
			this.exception = exception;
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
			catch (SQLException e)
			{
				try
				{
					SQLProxy.this.handleSQLException(e, this.database);
				}
				catch (SQLException exception)
				{
					this.handleException(e);
				}
			}
			catch (Throwable e)
			{
				this.handleException(e);
			}
		}
		
		private void handleException(Throwable e)
		{
			synchronized (this.exception)
			{
				if (this.exception.getCause() == null)
				{
					this.exception.initCause(e);
				}
			}
		}
	}
}
