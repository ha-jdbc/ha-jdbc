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
package net.sf.hajdbc.sql;

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.locks.Lock;

import net.sf.hajdbc.Balancer;
import net.sf.hajdbc.Database;
import net.sf.hajdbc.DatabaseCluster;
import net.sf.hajdbc.LockManager;
import net.sf.hajdbc.Messages;
import net.sf.hajdbc.Operation;
import net.sf.hajdbc.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base class for all HA-JDBC proxy objects.
 * @author  Paul Ferraro
 * @param <D> either java.sql.Driver or javax.sql.DataSource
 * @param <E> elements proxied by this object
 * @param <P> parent object that created the elements proxied by this object
 * @since   1.0
 */
public abstract class SQLObject<D, E, P>
{
	private static Logger logger = LoggerFactory.getLogger(SQLObject.class);
	
	private DatabaseCluster<D> databaseCluster;
	protected SQLObject<D, P, ?> parent;
	private Operation<D, P, E> parentOperation;
	private Map<Database<D>, E> objectMap;
	private Map<String, Operation<D, E, ?>> operationMap = new HashMap<String, Operation<D, E, ?>>();
	
	protected SQLObject(SQLObject<D, P, ?> parent, Operation<D, P, E> operation, ExecutorService executor, List<Lock> lockList) throws java.sql.SQLException
	{
		this(parent.getDatabaseCluster(), execute(parent, operation, executor, lockList));
		
		this.parent = parent;
		this.parentOperation = operation;
	}
	
	protected SQLObject(SQLObject<D, P, ?> parent, Operation<D, P, E> operation, ExecutorService executor) throws java.sql.SQLException
	{
		this(parent, operation, executor, null);
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
	private static <D, T, S> Map<Database<D>, T> execute(SQLObject<D, S, ?> parent, Operation<D, S, T> operation, ExecutorService executor, List<Lock> lockList) throws java.sql.SQLException
	{
		return parent.executeWriteToDatabase(operation, executor, lockList);
	}
	
	protected SQLObject(DatabaseCluster<D> databaseCluster, Map<Database<D>, E> objectMap)
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
	public synchronized final E getObject(Database<D> database)
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
				
				for (Operation<D, E, ?> operation: this.operationMap.values())
				{
					operation.execute(database, object);
				}
				
				this.objectMap.put(database, object);
			}
			catch (java.sql.SQLException e)
			{
				if (this.databaseCluster.deactivate(database))
				{
					logger.warn(Messages.getMessage(Messages.SQL_OBJECT_INIT_FAILED, this.getClass().getName(), database), e);
				}
			}
		}
		
		return object;
	}
	
	/**
	 * Records an operation.
	 * @param operation a database operation
	 */
	protected synchronized final void record(Operation<D, E, ?> operation)
	{
		this.operationMap.put(operation.getClass().toString(), operation);
	}
	
	private Set<Database<D>> getActiveDatabaseSet()
	{
		Set<Database<D>> databaseSet = this.databaseCluster.getBalancer().all();
		
		this.retain(databaseSet);
		
		return databaseSet;
	}
	
	protected synchronized void retain(Set<Database<D>> databaseSet)
	{
		if (this.parent == null) return;
		
		Iterator<Map.Entry<Database<D>, E>> mapEntries = this.objectMap.entrySet().iterator();
		
		while (mapEntries.hasNext())
		{
			Map.Entry<Database<D>, E> mapEntry = mapEntries.next();
			
			Database database = mapEntry.getKey();
			
			if (!databaseSet.contains(database))
			{
				E object = mapEntry.getValue();
				
				if (object != null)
				{
					try
					{
						this.close(object);
					}
					catch (java.sql.SQLException e)
					{
						// Ignore
					}
				}
				
				mapEntries.remove();
			}
		}
		
		this.parent.retain(databaseSet);
	}
	
	protected abstract void close(E object) throws java.sql.SQLException;
	
	/**
	 * Helper method that extracts the first result from a map of results.
	 * @param <T> 
	 * @param valueMap a Map<Database, Object> of operation execution results.
	 * @return a operation execution result
	 */
	protected final <T> T firstValue(Map<Database<D>, T> valueMap)
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
	public final <T> T executeReadFromDriver(Operation<D, E, T> operation) throws java.sql.SQLException
	{
		try
		{
			Database<D> database = this.databaseCluster.getBalancer().first();
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
	public final <T> T executeReadFromDatabase(Operation<D, E, T> operation) throws java.sql.SQLException
	{
		Balancer<D> balancer = this.databaseCluster.getBalancer();
		
		try
		{
			while (true)
			{
				Database<D> database = balancer.next();
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

	protected static List<Lock> emptyLockList()
	{
		return Collections.emptyList();
	}
	
	/**
	 * Executes the specified transactional write operation on every database in the cluster.
	 * It is assumed that these types of operation will require access to the database.
	 * @param <T> 
	 * @param operation a database operation
	 * @return the result of the operation
	 * @throws java.sql.SQLException if operation execution fails
	 */
	public final <T> Map<Database<D>, T> executeTransactionalWriteToDatabase(final Operation<D, E, T> operation) throws java.sql.SQLException
	{
		return this.executeTransactionalWriteToDatabase(operation, emptyLockList());
	}
	
	/**
	 * Executes the specified transactional write operation on every database in the cluster.
	 * It is assumed that these types of operation will require access to the database.
	 * @param <T> 
	 * @param operation a database operation
	 * @param lockList a list of locks
	 * @return the result of the operation
	 * @throws java.sql.SQLException if operation execution fails
	 */
	public final <T> Map<Database<D>, T> executeTransactionalWriteToDatabase(final Operation<D, E, T> operation, List<Lock> lockList) throws java.sql.SQLException
	{
		return this.executeWriteToDatabase(operation, this.databaseCluster.getTransactionalExecutor(), lockList);
	}
	
	/**
	 * Executes the specified non-transactional write operation on every database in the cluster.
	 * It is assumed that these types of operation will require access to the database.
	 * @param <T> 
	 * @param operation a database operation
	 * @return the result of the operation
	 * @throws java.sql.SQLException if operation execution fails
	 */
	public final <T> Map<Database<D>, T> executeNonTransactionalWriteToDatabase(final Operation<D, E, T> operation) throws java.sql.SQLException
	{
		return this.executeWriteToDatabase(operation, this.databaseCluster.getNonTransactionalExecutor(), null);
	}
	
	private <T> Map<Database<D>, T> executeWriteToDatabase(final Operation<D, E, T> operation, ExecutorService executor, List<Lock> lockList) throws java.sql.SQLException
	{
		Map<Database<D>, T> resultMap = new TreeMap<Database<D>, T>();
		SortedMap<Database<D>, java.sql.SQLException> exceptionMap = new TreeMap<Database<D>, java.sql.SQLException>();
		
		if (lockList != null)
		{
			if (lockList.isEmpty())
			{
				lockList = Collections.singletonList(this.databaseCluster.getLockManager().readLock(LockManager.GLOBAL));
			}
			
			for (Lock lock: lockList)
			{
				lock.lock();
			}
		}
		
		try
		{
			Set<Database<D>> databaseSet = this.getActiveDatabaseSet();
			
			if (databaseSet.isEmpty())
			{
				throw new SQLException(Messages.getMessage(Messages.NO_ACTIVE_DATABASES, this.databaseCluster));
			}
			
			Map<Database, Future<T>> futureMap = new HashMap<Database, Future<T>>();

			for (final Database<D> database: databaseSet)
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

			for (Database<D> database: databaseSet)
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
			if (lockList != null)
			{
				for (Lock lock: lockList)
				{
					lock.unlock();
				}
			}
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
	public final <T> Map<Database<D>, T> executeWriteToDriver(Operation<D, E, T> operation) throws java.sql.SQLException
	{
		Map<Database<D>, T> resultMap = new TreeMap<Database<D>, T>();

		Set<Database<D>> databaseSet = this.getActiveDatabaseSet();
		
		if (databaseSet.isEmpty())
		{
			throw new SQLException(Messages.getMessage(Messages.NO_ACTIVE_DATABASES, this.databaseCluster));
		}
		
		for (Database<D> database: databaseSet)
		{
			E object = this.getObject(database);
			
			resultMap.put(database, operation.execute(database, object));
		}
		
		this.record(operation);
		
		return resultMap;
	}
	
	/**
	 * Returns the database cluster to which this proxy is associated.
	 * @return a database cluster
	 */
	public DatabaseCluster<D> getDatabaseCluster()
	{
		return this.databaseCluster;
	}
	
	/**
	 * @param exceptionMap
	 * @throws java.sql.SQLException
	 */
	@SuppressWarnings("unused")
	public void handleExceptions(Map<Database<D>, java.sql.SQLException> exceptionMap) throws java.sql.SQLException
	{
		for (Map.Entry<Database<D>, java.sql.SQLException> exceptionMapEntry: exceptionMap.entrySet())
		{
			Database<D> database = exceptionMapEntry.getKey();
			java.sql.SQLException exception = exceptionMapEntry.getValue();
			
			if (this.databaseCluster.deactivate(database))
			{
				logger.error(Messages.getMessage(Messages.DATABASE_DEACTIVATED, database, this.databaseCluster), exception);
			}
		}
	}
}
