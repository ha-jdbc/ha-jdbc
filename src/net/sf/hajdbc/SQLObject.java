/*
 * HA-JDBC: High-Availability JDBC
 * Copyright (c) 2004-2006 Paul Ferraro
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
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.locks.Lock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base class for all HA-JDBC proxy objects.
 * @author  Paul Ferraro
 * @param <E> elements proxied by this object
 * @param <P> parent object that created the elements proxied by this object
 * @since   1.0
 */
public class SQLObject<E, P>
{
	private static Logger logger = LoggerFactory.getLogger(SQLObject.class);
	
	private DatabaseCluster databaseCluster;
	protected SQLObject<P, ?> parent;
	private Operation<P, E> parentOperation;
	private Map<Database, E> objectMap;
	private Map<String, Operation<E, ?>> operationMap = new HashMap<String, Operation<E, ?>>();
	
	protected SQLObject(SQLObject<P, ?> parent, Operation<P, E> operation, ExecutorService executor) throws java.sql.SQLException
	{
		this(parent.getDatabaseCluster(), execute(parent, operation, executor));
		
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
	private static <T, S> Map<Database, T> execute(SQLObject<S, ?> parent, Operation<S, T> operation, ExecutorService executor) throws java.sql.SQLException
	{
		return parent.executeWriteToDatabase(operation, executor);
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
				logger.warn(Messages.getMessage(Messages.SQL_OBJECT_INIT_FAILED, this.getClass().getName(), database), e);
				
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
	 * Executes the specified transactional write operation on every database in the cluster.
	 * It is assumed that these types of operation will require access to the database.
	 * @param <T> 
	 * @param operation a database operation
	 * @return the result of the operation
	 * @throws java.sql.SQLException if operation execution fails
	 */
	public final <T> Map<Database, T> executeTransactionalWriteToDatabase(final Operation<E, T> operation) throws java.sql.SQLException
	{
		return this.executeWriteToDatabase(operation, this.databaseCluster.getTransactionalExecutor());
	}
	
	/**
	 * Executes the specified non-transactional write operation on every database in the cluster.
	 * It is assumed that these types of operation will require access to the database.
	 * @param <T> 
	 * @param operation a database operation
	 * @return the result of the operation
	 * @throws java.sql.SQLException if operation execution fails
	 */
	public final <T> Map<Database, T> executeNonTransactionalWriteToDatabase(final Operation<E, T> operation) throws java.sql.SQLException
	{
		return this.executeWriteToDatabase(operation, this.databaseCluster.getNonTransactionalExecutor());
	}
	
	private <T> Map<Database, T> executeWriteToDatabase(final Operation<E, T> operation, ExecutorService executor) throws java.sql.SQLException
	{
		Map<Database, T> resultMap = new TreeMap<Database, T>();
		SortedMap<Database, java.sql.SQLException> exceptionMap = new TreeMap<Database, java.sql.SQLException>();
		
		Lock lock = this.databaseCluster.readLock();
		
		lock.lock();
		
		try
		{
			List<Database> databaseList = this.databaseCluster.getBalancer().list();
			
			if (databaseList.isEmpty())
			{
				throw new SQLException(Messages.getMessage(Messages.NO_ACTIVE_DATABASES, this.databaseCluster));
			}
			
			Map<Database, Future<T>> futureMap = new HashMap<Database, Future<T>>();

			for (final Database database: databaseList)
			{
				final E object = this.getObject(database);
				
				Callable<T> task = new Callable<T>()
				{
					public T call() throws java.sql.SQLException
					{
						return operation.execute(database, object);
					}
				};
	
				futureMap.put(database, executor.submit(task));
			}

			for (Database database: databaseList)
			{
				Future<T> future = futureMap.get(database);
				
				try
				{
					resultMap.put(database, future.get());
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
			}
		}
		catch (InterruptedException e)
		{
			throw new SQLException(e);
		}
		finally
		{
			lock.unlock();
		}
		
		// If no databases returned successfully, return an exception back to the caller
		if (resultMap.isEmpty())
		{
			if (exceptionMap.isEmpty())
			{
				throw new SQLException(Messages.getMessage(Messages.NO_ACTIVE_DATABASES, this.databaseCluster));
			}
			
			throw exceptionMap.get(exceptionMap.firstKey());
		}
		
		// If any databases failed, while others succeeded, deactivate them
		if (!exceptionMap.isEmpty())
		{
			this.handleExceptions(exceptionMap);
		}
		
		// Return results from successful operations
		return resultMap;
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
		Map<Database, T> resultMap = new TreeMap<Database, T>();

		Lock lock = this.databaseCluster.readLock();

		lock.lock();
		
		try
		{
			List<Database> databaseList = this.databaseCluster.getBalancer().list();
			
			if (databaseList.isEmpty())
			{
				throw new SQLException(Messages.getMessage(Messages.NO_ACTIVE_DATABASES, this.databaseCluster));
			}

			for (Database database: databaseList)
			{
				E object = this.getObject(database);
				
				resultMap.put(database, operation.execute(database, object));
			}
		}
		finally
		{
			lock.unlock();
		}
		
		this.record(operation);
		
		return resultMap;
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
				logger.error(Messages.getMessage(Messages.DATABASE_DEACTIVATED, database, this.databaseCluster), exception);
			}
		}
	}
}
