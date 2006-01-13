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

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

/**
 * @author  Paul Ferraro
 * @param <E> 
 * @param <P> 
 * @since   1.0
 */
public class SQLObject<E, P>
{
	private static Log log = LogFactory.getLog(SQLObject.class);
	
	private DatabaseCluster databaseCluster;
	protected SQLObject<P, ?> parent;
	private Operation<P, E> parentOperation;
	private Map<Database, E> objectMap;
	private Map<String, Operation<E, ?>> operationMap = new HashMap<String, Operation<E, ?>>();
	
	protected SQLObject(SQLObject<P, ?> parent, Operation<P, E> operation) throws java.sql.SQLException
	{
		this(parent.getDatabaseCluster(), execute(parent, operation));
		
		this.parent = parent;
		this.parentOperation = operation;
	}
	
	/**
	 * Temporary static method to work around bug in Eclipse compiler
	 * @param parent 
	 * @param operation 
	 * @param <T> 
	 * @param <S> 
	 * @return map of Database to SQL object
	 * @throws java.sql.SQLException 
	 */
	private static <T, S> Map<Database, T> execute(SQLObject<S, ?> parent, Operation<S, T> operation) throws java.sql.SQLException
	{
		return parent.executeWriteToDatabase(operation);
	}
	
	protected SQLObject(DatabaseCluster databaseCluster, Map<Database, E> objectMap)
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
	public synchronized final E getObject(Database database)
	{
		E object = this.objectMap.get(database);
		
		if (object == null)
		{
			try
			{
				if (this.parent == null)
				{
					throw new java.sql.SQLException();
				}
				
				P parentObject = this.parent.getObject(database);
				
				if (parentObject == null)
				{
					throw new java.sql.SQLException();
				}
				
				object = this.parentOperation.execute(database, parentObject);
				
				for (Operation operation: this.operationMap.values())
				{
					operation.execute(database, object);
				}
				
				this.objectMap.put(database, object);
			}
			catch (java.sql.SQLException e)
			{
				log.warn(Messages.getMessage(Messages.SQL_OBJECT_INIT_FAILED, this.getClass().getName(), database), e);
				
				this.databaseCluster.deactivate(database);
			}
		}
		
		return object;
	}
	
	/**
	 * Records an operation.
	 * @param operation a database operation
	 */
	protected synchronized final void record(Operation<E, ?> operation)
	{
		this.operationMap.put(operation.getClass().toString(), operation);
	}
	
	/**
	 * Helper method that extracts the first result from a map of results.
	 * @param <T> 
	 * @param valueMap a Map<Database, Object> of operation execution results.
	 * @return a operation execution result
	 */
	protected final <T> T firstValue(Map<Database, T> valueMap)
	{
		return valueMap.values().iterator().next();
	}

	/**
	 * Executes the specified read operation on a single database in the cluster.
	 * It is assumed that these types of operation will <em>not</em> require access to the database.
	 * @param <T> 
	 * @param operation a database operation
	 * @return the result of the operation
	 * @throws java.sql.SQLException if operation execution fails
	 */
	public final <T> T executeReadFromDriver(Operation<E, T> operation) throws java.sql.SQLException
	{
		try
		{
			Database database = this.databaseCluster.getBalancer().first();
			E object = this.getObject(database);
			
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
	 * @param <T> 
	 * @param operation a database operation
	 * @return the result of the operation
	 * @throws java.sql.SQLException if operation execution fails
	 */
	public final <T> T executeReadFromDatabase(Operation<E, T> operation) throws java.sql.SQLException
	{
		Balancer balancer = this.databaseCluster.getBalancer();
		
		try
		{
			while (true)
			{
				Database database = balancer.next();
				E object = this.getObject(database);
	
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
	 * @param <T> 
	 * @param operation a database operation
	 * @return the result of the operation
	 * @throws java.sql.SQLException if operation execution fails
	 */
	public final <T> Map<Database, T> executeWriteToDatabase(final Operation<E, T> operation) throws java.sql.SQLException
	{
		ExecutorService executor = this.databaseCluster.getExecutor();
		List<Database> databases = this.getDatabaseList();
		Map<Database, Future<T>> futureMap = new HashMap<Database, Future<T>>();
		
		for (final Database database: databases)
		{
			final E object = this.getObject(database);
			
			Callable<T> callable = new Callable<T>()
			{
				public T call() throws java.sql.SQLException
				{
					return operation.execute(database, object);
				}
			};

			futureMap.put(database, executor.submit(callable));
		}

		Map<Database, T> returnValueMap = new HashMap<Database, T>();
		Map<Database, java.sql.SQLException> exceptionMap = new HashMap<Database, java.sql.SQLException>();
		
		for (Database database: databases)
		{
			Future<T> future = futureMap.get(database);
			
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
			
			throw exceptionMap.values().iterator().next();
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
	 * Executes the specified write operation on every database in the cluster.
	 * It is assumed that these types of operation will <em>not</em> require access to the database.
	 * @param <T> 
	 * @param operation a database operation
	 * @return the result of the operation
	 * @throws java.sql.SQLException if operation execution fails
	 */
	public final <T> Map<Database, T> executeWriteToDriver(Operation<E, T> operation) throws java.sql.SQLException
	{
		List<Database> databases = this.getDatabaseList();
		
		Map<Database, T> returnValueMap = new HashMap<Database, T>();

		for (Database database: databases)
		{
			E object = this.getObject(database);
			
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
	public void handleExceptions(Map<Database, java.sql.SQLException> exceptionMap) throws java.sql.SQLException
	{
		for (Map.Entry<Database, java.sql.SQLException> exceptionMapEntry: exceptionMap.entrySet())
		{
			Database database = exceptionMapEntry.getKey();
			java.sql.SQLException exception = exceptionMapEntry.getValue();
			
			if (this.databaseCluster.deactivate(database))
			{
				log.error(Messages.getMessage(Messages.DATABASE_DEACTIVATED, database, this.databaseCluster), exception);
			}
		}
	}

	private void deactivateNewDatabases(Collection<Database> databases)
	{
		Set<Database> databaseSet = new HashSet<Database>(this.databaseCluster.getBalancer().list());
		
		for (Database database: databases)
		{
			databaseSet.remove(database);
		}
		
		for (Database database: databaseSet)
		{
			if (this.databaseCluster.deactivate(database))
			{
				log.error(Messages.getMessage(Messages.DATABASE_DEACTIVATED, database, this.databaseCluster));
			}
		}
	}
	
	private List<Database> getDatabaseList() throws SQLException
	{
		List<Database> databaseList = this.databaseCluster.getBalancer().list();
		
		if (databaseList.isEmpty())
		{
			throw new SQLException(Messages.getMessage(Messages.NO_ACTIVE_DATABASES, this.databaseCluster));
		}
		
		return databaseList;
	}
}
