package net.sf.ha.jdbc;

import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * @author  Paul Ferraro
 * @version $Revision$
 * @since   1.0
 */
public abstract class AbstractProxy
{
	protected Map objectMap;
	
	protected AbstractProxy(Map objectMap)
	{
		this.objectMap = Collections.synchronizedMap(objectMap);
	}
	
	public final Object executeRead(Operation operation) throws SQLException
	{
		Database database = null;
		
		synchronized (this.objectMap)
		{
			database = (Database) this.objectMap.keySet().iterator().next();
		}

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
	
	public final Object firstItem(Map returnValueMap)
	{
		return returnValueMap.values().iterator().next();
	}
	
	public final Map executeWrite(Operation operation) throws SQLException
	{
		Map returnValueMap = null;
		Set databaseSet = null;
		
		synchronized (this.objectMap)
		{
			if (this.objectMap.size() == 0)
			{
				throw new SQLException("No available connection");
			}
			
			returnValueMap = new HashMap(this.objectMap.size());
			databaseSet = new HashSet(this.objectMap.keySet());
			
			Iterator objectMapEntries = this.objectMap.entrySet().iterator();
			
			while (objectMapEntries.hasNext())
			{
				Map.Entry objectMapEntry = (Map.Entry) objectMapEntries.next();
				Database database = (Database) objectMapEntry.getKey();
				Object object = objectMapEntry.getValue();
				
				Executor executor = new Executor(operation, database, object, returnValueMap, databaseSet);
				
				databaseSet.add(database);
				
				new Thread(executor).start();
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
					// Ignore
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
		this.objectMap.remove(database);
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
					AbstractProxy.this.handleSQLException(e, this.database);
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
