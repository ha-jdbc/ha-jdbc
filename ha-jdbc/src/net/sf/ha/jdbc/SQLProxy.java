package net.sf.ha.jdbc;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/**
 * @author  Paul Ferraro
 * @version $Revision$
 * @since   1.0
 */
public abstract class SQLProxy
{
	private Map objectMap;
	
	protected SQLProxy(Map objectMap)
	{
		this.objectMap = Collections.synchronizedMap(objectMap);
	}
	
	public Object getObject(Database database)
	{
		return this.objectMap.get(database);
	}
	
	public final Object executeRead(Operation operation) throws SQLException
	{
		Database database = null;
		Object object = null;
		
		synchronized (this.objectMap)
		{
			if (this.objectMap.size() == 0)
			{
				throw new SQLException("No available databases");
			}
			
			database = (Database) this.objectMap.keySet().iterator().next();
			object = this.objectMap.get(database);
		}
		
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
		Map.Entry objectMapEntry = null;
		
		synchronized (this.objectMap)
		{
			if (this.objectMap.size() == 0)
			{
				throw new SQLException("No available databases");
			}
			
			objectMapEntry = (Map.Entry) this.objectMap.entrySet().iterator().next();
		}
		
		Database database = (Database) objectMapEntry.getKey();
		Object object = objectMapEntry.getValue();
		
		return operation.execute(database, object);
	}
	
	public final Object firstItem(Map returnValueMap)
	{
		return returnValueMap.values().iterator().next();
	}
	
	public final Map executeWrite(Operation operation) throws SQLException
	{
		Set databaseSet = new HashSet(this.objectMap.keySet());
		
		if (databaseSet.size() == 0)
		{
			throw new SQLException("No available databases");
		}
		
		Map returnValueMap = new LinkedHashMap(databaseSet.size(), 0.75f, true);
		
		Iterator databases = new ArrayList(databaseSet).iterator();
		
		while (databases.hasNext())
		{
			Database database = (Database) databases.next();
			Object object = this.objectMap.get(database);
			
			if (object != null)
			{
				Executor executor = new Executor(operation, database, object, returnValueMap, databaseSet);
				
				new Thread(executor).start();
			}
			else
			{
				synchronized (databaseSet)
				{
					databaseSet.remove(database);
				}
			}
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

		if (returnValueMap.size() == 0)
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
		Set objectMapEntrySet = new HashSet(this.objectMap.entrySet());
		
		if (objectMapEntrySet.size() == 0)
		{
			throw new SQLException("No available databases");
		}
		
		Map returnValueMap = new LinkedHashMap(objectMapEntrySet.size(), 0.75f, true);
		
		Iterator objectMapEntries = objectMapEntrySet.iterator();
		
		while (objectMapEntries.hasNext())
		{
			Map.Entry objectMapEntry = (Map.Entry) objectMapEntries.next();
			Database database = (Database) objectMapEntry.getKey();
			Object object = objectMapEntry.getValue();
			
			if (object != null)
			{
				Object returnValue = operation.execute(database, object);
				
				returnValueMap.put(database, returnValue);
			}
		}
		
		return returnValueMap;
	}
	
	protected final void handleSQLException(SQLException exception, Database database) throws SQLException
	{
		if (this.getDatabaseCluster().isActive(database))
		{
			throw exception;
		}
		
		this.deactivate(database);
	}

	protected abstract DatabaseCluster getDatabaseCluster();
	
	public void deactivate(Database database)
	{
		synchronized (this.objectMap)
		{
			this.objectMap.remove(database);
		}
	}

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
					this.returnValueMap.put(this.database, e);
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
