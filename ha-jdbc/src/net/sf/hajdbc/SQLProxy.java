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
import java.util.HashSet;
import java.util.Iterator;
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
	
	private Map objectMap;
	
	protected SQLProxy(Map objectMap)
	{
		this.objectMap = objectMap;
	}
	
	public Object getObject(Database database)
	{
		return this.objectMap.get(database);
	}
	
	public final Object executeRead(Operation operation) throws SQLException
	{
		Database database = this.getDatabaseCluster().getDescriptor().nextDatabase();
		Object object = this.objectMap.get(database);
		
		try
		{
			return operation.execute(database, object);
		}
		catch (SQLException e)
		{
			this.handleSQLException(e, database);
			
			return this.executeRead(operation);
		}
	}
	
	public final Object executeGet(Operation operation) throws SQLException
	{
		Database database = this.getDatabaseCluster().getDescriptor().firstDatabase();
		Object object = this.objectMap.get(database);
		
		return operation.execute(database, object);
	}
	
	public final Object firstItem(Map returnValueMap)
	{
		return returnValueMap.values().iterator().next();
	}
	
	public final Map executeWrite(Operation operation) throws SQLException
	{
		List databaseList = this.getDatabaseCluster().getDescriptor().getActiveDatabaseList();
		Set databaseSet = new HashSet(databaseList);
		
		Map returnValueMap = new HashMap(databaseList.size());

		for (int i = 0; i < databaseList.size(); ++i)
		{
			Database database = (Database) databaseList.get(i);
			Object object = this.objectMap.get(database);
			
			Executor executor = new Executor(operation, database, object, returnValueMap, databaseSet);
			
			new Thread(executor).start();
		}
		
		// Wait until all threads have completed
		synchronized (databaseSet)
		{
			while (databaseSet.size() > 0)
			{
				try
				{
					databaseSet.wait();
				}
				catch (InterruptedException e)
				{
					throw new SQLException("Interruption during execution.");
				}
			}
		}

		if (returnValueMap.isEmpty())
		{
			throw new SQLException("No active database connection available");
		}
		
		Iterator returnValues = returnValueMap.values().iterator();
		
		while (returnValues.hasNext())
		{
			Object object = returnValues.next();
			
			if (SQLException.class.isInstance(object))
			{
				throw (SQLException) object;
			}
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
			Object object = this.objectMap.get(database);
			
			Object returnValue = operation.execute(database, object);
			
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
		
		databaseCluster.deactivate(database);
		
		log.error("Database " + database.getId() + " of cluster " + this.getDatabaseCluster().getDescriptor().getName() + " was deactivated.", exception);
	}

	protected abstract DatabaseCluster getDatabaseCluster();

	private class Executor implements Runnable
	{
		private Operation operation;
		private Database database;
		private Object object;
		private Map returnValueMap;
		private Set databaseSet;
		
		public Executor(Operation operation, Database database, Object object, Map returnValueMap, Set databaseSet)
		{
			this.operation = operation;
			this.database = database;
			this.object = object;
			this.returnValueMap = returnValueMap;
			this.databaseSet = databaseSet;
		}
		
		public void run()
		{
			try
			{
				Object returnValue = this.operation.execute(this.database, this.object);
				
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
					synchronized (this.returnValueMap)
					{
						this.returnValueMap.put(this.database, e);
					}
				}
			}
			finally
			{
				synchronized (this.databaseSet)
				{
					this.databaseSet.remove(this.database);
					this.databaseSet.notify();
				}
			}
		}
	}
}
