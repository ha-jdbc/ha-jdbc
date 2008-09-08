/*
 * HA-JDBC: High-Availability JDBC
 * Copyright (c) 2004-2007 Paul Ferraro
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

import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeSet;
import java.util.concurrent.locks.Lock;

import net.sf.hajdbc.Database;
import net.sf.hajdbc.LockManager;
import net.sf.hajdbc.Messages;
import net.sf.hajdbc.TableProperties;
import net.sf.hajdbc.util.SQLExceptionFactory;
import net.sf.hajdbc.util.reflect.Methods;

/**
 * @author Paul Ferraro
 * @param <D> 
 * @param <S> 
 */
@SuppressWarnings("nls")
public abstract class AbstractStatementInvocationHandler<D, S extends Statement> extends AbstractChildInvocationHandler<D, Connection, S>
{
	private static final Set<Method> driverReadMethodSet = Methods.findMethods(Statement.class, "getFetchDirection", "getFetchSize", "getGeneratedKeys", "getMaxFieldSize", "getMaxRows", "getQueryTimeout", "getResultSetConcurrency", "getResultSetHoldability", "getResultSetType", "getUpdateCount", "getWarnings", "isClosed", "isPoolable");
	private static final Set<Method> driverWriteMethodSet = Methods.findMethods(Statement.class, "addBatch", "clearBatch", "clearWarnings", "setCursorName", "setEscapeProcessing", "setFetchDirection", "setFetchSize", "setMaxFieldSize", "setMaxRows", "setPoolable", "setQueryTimeout");
	private static final Set<Method> executeMethodSet = Methods.findMethods(Statement.class, "execute(Update)?");
	
	private static final Method getConnectionMethod = Methods.getMethod(Statement.class, "getConnection");
	private static final Method executeQueryMethod = Methods.getMethod(Statement.class, "executeQuery", String.class);
	private static final Method clearBatchMethod = Methods.getMethod(Statement.class, "clearBatch");
	private static final Method executeBatchMethod = Methods.getMethod(Statement.class, "executeBatch");
	private static final Method getMoreResultsMethod = Methods.getMethod(Statement.class, "getMoreResults", Integer.TYPE);
	private static final Method getResultSetMethod = Methods.getMethod(Statement.class, "getResultSet");
	private static final Method addBatchMethod = Methods.getMethod(Statement.class, "addBatch", String.class);
	private static final Method closeMethod = Methods.getMethod(Statement.class, "close");
	
	protected TransactionContext<D> transactionContext;
	protected FileSupport fileSupport;
	
	private List<Invoker<D, S, ?>> invokerList = new LinkedList<Invoker<D, S, ?>>();
	private List<String> sqlList = new LinkedList<String>();
	
	/**
	 * @param connection the parent connection of this statement
	 * @param proxy the parent invocation handler
	 * @param invoker the invoker that created this statement
	 * @param statementClass 
	 * @param statementMap a map of database to underlying statement
	 * @param transactionContext 
	 * @param fileSupport support object for streams
	 * @throws Exception
	 */
	protected AbstractStatementInvocationHandler(Connection connection, SQLProxy<D, Connection> proxy, Invoker<D, Connection, S> invoker, Class<S> statementClass, Map<Database<D>, S> statementMap, TransactionContext<D> transactionContext, FileSupport fileSupport) throws Exception
	{
		super(connection, proxy, invoker, statementClass, statementMap);
		
		this.transactionContext = transactionContext;
		this.fileSupport = fileSupport;
	}

	/**
	 * @see net.sf.hajdbc.sql.AbstractChildInvocationHandler#getInvocationStrategy(java.lang.Object, java.lang.reflect.Method, java.lang.Object[])
	 */
	@Override
	protected InvocationStrategy<D, S, ?> getInvocationStrategy(S statement, Method method, Object[] parameters) throws Exception
	{
		if (driverReadMethodSet.contains(method))
		{
			return new DriverReadInvocationStrategy<D, S, Object>();
		}
		
		if (driverWriteMethodSet.contains(method) || method.equals(closeMethod))
		{
			return new DriverWriteInvocationStrategy<D, S, Object>();
		}
		
		if (executeMethodSet.contains(method))
		{
			List<Lock> lockList = this.extractLocks((String) parameters[0]);
			
			return this.transactionContext.start(new LockingInvocationStrategy<D, S, Object>(new DatabaseWriteInvocationStrategy<D, S, Object>(this.cluster.getTransactionalExecutor()), lockList), this.getParent());
		}
		
		if (method.equals(getConnectionMethod))
		{
			return new InvocationStrategy<D, S, Connection>()
			{
				public Connection invoke(SQLProxy<D, S> proxy, Invoker<D, S, Connection> invoker) throws Exception
				{
					return AbstractStatementInvocationHandler.this.getParent();
				}
			};
		}
		
		if (method.equals(executeQueryMethod))
		{
			String sql = (String) parameters[0];
			
			List<Lock> lockList = this.extractLocks(sql);
			
			int concurrency = statement.getResultSetConcurrency();
			boolean selectForUpdate = this.isSelectForUpdate(sql);
			
			if (lockList.isEmpty() && (concurrency == ResultSet.CONCUR_READ_ONLY) && !selectForUpdate)
			{
				return new LazyResultSetInvocationStrategy<D, S>(statement, this.transactionContext, this.fileSupport);
			}
			
			InvocationStrategy<D, S, ResultSet> strategy = new LockingInvocationStrategy<D, S, ResultSet>(new EagerResultSetInvocationStrategy<D, S>(this.cluster, statement, this.transactionContext, this.fileSupport), lockList);
			
			return selectForUpdate ? this.transactionContext.start(strategy, this.getParent()) : strategy;
		}
		
		if (method.equals(executeBatchMethod))
		{
			List<Lock> lockList = this.extractLocks(this.sqlList);
			
			return this.transactionContext.start(new LockingInvocationStrategy<D, S, Object>(new DatabaseWriteInvocationStrategy<D, S, Object>(this.cluster.getTransactionalExecutor()), lockList), this.getParent());
		}
		
		if (method.equals(getMoreResultsMethod))
		{
			if (parameters[0].equals(Statement.KEEP_CURRENT_RESULT))
			{
				return new DriverWriteInvocationStrategy<D, S, Object>();
			}
		}
		
		if (method.equals(getResultSetMethod))
		{
			if (statement.getResultSetConcurrency() == ResultSet.CONCUR_READ_ONLY)
			{
				return new LazyResultSetInvocationStrategy<D, S>(statement, this.transactionContext, this.fileSupport);
			}
			
			return new EagerResultSetInvocationStrategy<D, S>(this.cluster, statement, this.transactionContext, this.fileSupport);
		}
		
		return super.getInvocationStrategy(statement, method, parameters);
	}

	/**
	 * @see net.sf.hajdbc.sql.AbstractChildInvocationHandler#isSQLMethod(java.lang.reflect.Method)
	 */
	@Override
	protected boolean isSQLMethod(Method method)
	{
		return method.equals(addBatchMethod) || method.equals(executeQueryMethod) || executeMethodSet.contains(method);
	}

	/**
	 * @see net.sf.hajdbc.sql.AbstractChildInvocationHandler#postInvoke(java.lang.Object, java.lang.reflect.Method, java.lang.Object[])
	 */
	@Override
	protected void postInvoke(S statement, Method method, Object[] parameters)
	{
		if (method.equals(addBatchMethod))
		{
			this.sqlList.add((String) parameters[0]);
		}
		else if (method.equals(clearBatchMethod) || method.equals(executeBatchMethod))
		{
			this.sqlList.clear();
		}
		else if (method.equals(closeMethod))
		{
			this.fileSupport.close();
			
			this.getParentProxy().removeChild(this);
		}
	}

	/**
	 * @see net.sf.hajdbc.sql.SQLProxy#handlePartialFailure(java.util.SortedMap, java.util.SortedMap)
	 */
	@Override
	public <R> SortedMap<Database<D>, R> handlePartialFailure(SortedMap<Database<D>, R> resultMap, SortedMap<Database<D>, Exception> exceptionMap) throws Exception
	{
		if (this.getParent().getAutoCommit())
		{
			return super.handlePartialFailure(resultMap, exceptionMap);
		}
		
		// If auto-commit is off, throw exception to give client the opportunity to rollback the transaction
		Map<Boolean, List<Database<D>>> aliveMap = this.cluster.getAliveMap(exceptionMap.keySet());

		List<Database<D>> aliveList = aliveMap.get(true);

		int size = aliveList.size();
		
		// Assume successful databases are alive
		aliveList.addAll(resultMap.keySet());
		
		this.detectClusterPanic(aliveMap);
		
		List<Database<D>> deadList = aliveMap.get(false);
		
		for (Database<D> database: deadList)
		{
			if (this.cluster.deactivate(database, this.cluster.getStateManager()))
			{
				this.logger.error(Messages.getMessage(Messages.DATABASE_DEACTIVATED, database, this.cluster), exceptionMap.get(database));
			}
		}

		// If failed databases are all dead
		if (size == 0)
		{
			return resultMap;
		}
		
		// Chain exceptions from alive databases
		SQLException exception = SQLExceptionFactory.createSQLException(exceptionMap.get(aliveList.get(0)));
		
		for (Database<D> database: aliveList.subList(1, size))
		{
			exception.setNextException(SQLExceptionFactory.createSQLException(exceptionMap.get(database)));
		}
		
		throw exception;
	}
	
	protected boolean isSelectForUpdate(String sql) throws SQLException
	{
		return this.cluster.getDatabaseMetaDataCache().getDatabaseProperties(this.getParent()).supportsSelectForUpdate() ? this.cluster.getDialect().isSelectForUpdate(sql) : false;
	}
	
	protected List<Lock> extractLocks(String sql) throws SQLException
	{
		return this.extractLocks(Collections.singletonList(sql));
	}
	
	private List<Lock> extractLocks(List<String> sqlList) throws SQLException
	{
		Set<String> identifierSet = new TreeSet<String>();
		
		for (String sql: sqlList)
		{
			if (this.cluster.isSequenceDetectionEnabled())
			{
				String sequence = this.cluster.getDialect().parseSequence(sql);
				
				if (sequence != null)
				{
					identifierSet.add(sequence);
				}
			}
			
			if (this.cluster.isIdentityColumnDetectionEnabled())
			{
				String table = this.cluster.getDialect().parseInsertTable(sql);
				
				if (table != null)
				{
					TableProperties tableProperties = this.cluster.getDatabaseMetaDataCache().getDatabaseProperties(this.getParent()).findTable(table);
					
					if (!tableProperties.getIdentityColumns().isEmpty())
					{
						identifierSet.add(tableProperties.getName());
					}
				}
			}
		}

		List<Lock> lockList = new ArrayList<Lock>(identifierSet.size());
		
		if (!identifierSet.isEmpty())
		{
			LockManager lockManager = this.cluster.getLockManager();
			
			for (String identifier: identifierSet)
			{
				lockList.add(lockManager.writeLock(identifier));
			}
		}
		
		return lockList;
	}

	/**
	 * @see net.sf.hajdbc.sql.AbstractChildInvocationHandler#close(java.lang.Object, java.lang.Object)
	 */
	@Override
	protected void close(Connection connection, S statement) throws SQLException
	{
		statement.close();
	}

	/**
	 * @see net.sf.hajdbc.sql.AbstractInvocationHandler#record(java.lang.reflect.Method, net.sf.hajdbc.sql.Invoker)
	 */
	@Override
	protected void record(Method method, Invoker<D, S, ?> invoker)
	{
		if (this.isRecordable(method))
		{
			synchronized (this.invokerList)
			{
				this.invokerList.add(invoker);
			}
		}
		else if (method.equals(clearBatchMethod) || method.equals(executeBatchMethod))
		{
			synchronized (this.invokerList)
			{
				this.invokerList.clear();
			}
		}
		else
		{
			super.record(method, invoker);
		}
	}

	protected boolean isRecordable(Method method)
	{
		return method.equals(addBatchMethod);
	}
	
	/**
	 * @see net.sf.hajdbc.sql.AbstractInvocationHandler#replay(net.sf.hajdbc.Database, java.lang.Object)
	 */
	@Override
	protected void replay(Database<D> database, S statement) throws Exception
	{
		super.replay(database, statement);
		
		synchronized (this.invokerList)
		{
			for (Invoker<D, S, ?> invoker: this.invokerList)
			{
				invoker.invoke(database, statement);
			}
		}
	}
}
