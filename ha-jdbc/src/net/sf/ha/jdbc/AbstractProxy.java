package net.sf.ha.jdbc;

import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

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
			verifySize(this.objectMap);
			
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
		ThreadGroup threadGroup = new ThreadGroup(null);
		Map returnValueMap = Collections.synchronizedMap(new HashMap(this.objectMap.size()));
		
		synchronized (this.objectMap)
		{
			verifySize(this.objectMap);
			
			Iterator objectMapEntries = this.objectMap.entrySet().iterator();
			
			while (objectMapEntries.hasNext())
			{
				Map.Entry objectMapEntry = (Map.Entry) objectMapEntries.next();
				Database database = (Database) objectMapEntry.getKey();
				Object object = objectMapEntry.getValue();
				Executor executor = new Executor(operation, database, object, returnValueMap);
				
				new Thread(threadGroup, executor).start();
			}
		}
		
		// Wait until all threads have completed
		while (threadGroup.activeCount() > 0)
		{
			Thread.yield();
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
	
	private void verifySize(Map map) throws SQLException
	{
		if (map.size() == 0)
		{
			throw new SQLException("No available connection");
		}
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
		private Map returnValueMap;
		private Database database;
		private Object object;
		private Operation operation;
		
		public Executor(Operation operation, Database database, Object object, Map returnValueMap)
		{
			this.returnValueMap = returnValueMap;
			this.database = database;
			this.object = object;
			this.operation = operation;
		}
		
		public void run()
		{
			try
			{
				Object returnValue = this.operation.execute(this.database, this.object);
				
				this.returnValueMap.put(this.database, returnValue);
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
		}
	}
}
