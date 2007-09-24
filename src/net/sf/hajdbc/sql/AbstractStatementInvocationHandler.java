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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.locks.Lock;

import net.sf.hajdbc.Database;
import net.sf.hajdbc.DatabaseCluster;
import net.sf.hajdbc.Dialect;
import net.sf.hajdbc.LockManager;
import net.sf.hajdbc.Messages;
import net.sf.hajdbc.TableProperties;

/**
 * @author Paul Ferraro
 *
 */
@SuppressWarnings("nls")
public abstract class AbstractStatementInvocationHandler<D, S extends Statement> extends AbstractChildInvocationHandler<D, Connection, S>
{
	private static final Set<String> DRIVER_READ_METHOD_SET = new HashSet<String>(Arrays.asList("getFetchDirection", "getFetchSize", "getGeneratedKeys", "getMaxFieldSize", "getMaxRows", "getQueryTimeout", "getResultSetConcurrency", "getResultSetHoldability", "getResultSetType", "getUpdateCount", "getWarnings", "isClosed", "isPoolable"));
	private static final Set<String> DRIVER_WRITE_METHOD_SET = new HashSet<String>(Arrays.asList("addBatch", "clearBatch", "clearWarnings", "setCursorName", "setEscapeProcessing", "setFetchDirection", "setFetchSize", "setMaxFieldSize", "setMaxRows", "setPoolable", "setQueryTimeout"));
	
	private List<String> sqlList = new LinkedList<String>();
	protected FileSupport fileSupport;
	
	/**
	 * @param connection the parent connection of this statement
	 * @param proxy the parent invocation handler
	 * @param invoker the invoker that created this statement
	 * @param statementMap a map of database to underlying statement
	 * @param fileSupport support object for streams
	 * @throws Exception
	 */
	public AbstractStatementInvocationHandler(Connection connection, SQLProxy<D, Connection> proxy, Invoker<D, Connection, S> invoker, Class<S> statementClass, Map<Database<D>, S> statementMap, FileSupport fileSupport) throws Exception
	{
		super(connection, proxy, invoker, statementClass, statementMap);
		
		this.fileSupport = fileSupport;
	}

	/**
	 * @see net.sf.hajdbc.sql.AbstractChildInvocationHandler#getInvocationStrategy(java.lang.Object, java.lang.reflect.Method, java.lang.Object[])
	 */
	@Override
	protected InvocationStrategy<D, S, ?> getInvocationStrategy(S statement, Method method, Object[] parameters) throws Exception
	{
		String methodName = method.getName();
		
		if (DRIVER_READ_METHOD_SET.contains(methodName))
		{
			return new DriverReadInvocationStrategy<D, S, Object>();
		}
		
		if (DRIVER_WRITE_METHOD_SET.contains(methodName))
		{
			return new DriverWriteInvocationStrategy<D, S, Object>();
		}
		
		if (methodName.equals("execute") || methodName.equals("executeUpdate"))
		{
			Class<?>[] types = method.getParameterTypes();
			
			if ((types != null) && (types.length > 0) && types[0].equals(String.class))
			{
				return new DatabaseWriteInvocationStrategy<D, S, Object>(this.getLockList((String) parameters[0]));
			}
		}
		
		if (method.equals(Statement.class.getMethod("getConnection")))
		{
			return new InvocationStrategy<D, S, Connection>()
			{
				public Connection invoke(SQLProxy<D, S> proxy, Invoker<D, S, Connection> invoker) throws Exception
				{
					return AbstractStatementInvocationHandler.this.getParent();
				}
			};
		}
		
		if (method.equals(Statement.class.getMethod("executeQuery", String.class)))
		{
			String sql = (String) parameters[0];
			
			List<Lock> lockList = this.getLockList(sql);
			
			return (lockList.isEmpty() && (statement.getResultSetConcurrency() == java.sql.ResultSet.CONCUR_READ_ONLY) && !this.isSelectForUpdate(sql)) ? new LazyResultSetInvocationStrategy<D, S>(statement) : new EagerResultSetInvocationStrategy<D, S>(statement, this.fileSupport, lockList);
		}
		
		if (method.equals(Statement.class.getMethod("executeBatch")))
		{
			return new DatabaseWriteInvocationStrategy<D, S, Object>(this.getLockList(this.sqlList));
		}
		
		if (method.equals(Statement.class.getMethod("getMoreResults", Integer.TYPE)))
		{
			if (parameters[0].equals(Statement.KEEP_CURRENT_RESULT))
			{
				return new DriverWriteInvocationStrategy<D, S, Object>();
			}
		}
		
		if (method.equals(Statement.class.getMethod("getResultSet")))
		{
			if (statement.getResultSetConcurrency() == ResultSet.CONCUR_READ_ONLY)
			{
				return new LazyResultSetInvocationStrategy<D, S>(statement);
			}
			
			List<Lock> lockList = Collections.emptyList();
			
			return new EagerResultSetInvocationStrategy<D, S>(statement, this.fileSupport, lockList);
		}
		
		return super.getInvocationStrategy(statement, method, parameters);
	}

	/**
	 * @see net.sf.hajdbc.sql.AbstractChildInvocationHandler#isSQLMethod(java.lang.reflect.Method)
	 */
	@Override
	protected boolean isSQLMethod(Method method)
	{
		String methodName = method.getName();
		Class<?>[] parameterTypes = method.getParameterTypes();
		
		return (methodName.equals("addBatch") || methodName.equals("executeQuery") || methodName.equals("execute") || methodName.equals("executeUpdate")) && (parameterTypes.length > 0) && parameterTypes[0].equals(String.class);
	}

	/**
	 * @see net.sf.hajdbc.sql.AbstractChildInvocationHandler#postInvoke(java.lang.Object, java.lang.reflect.Method, java.lang.Object[])
	 */
	@Override
	protected void postInvoke(S statement, Method method, Object[] parameters) throws Exception
	{
		if (method.equals(Statement.class.getMethod("addBatch", String.class)))
		{
			this.sqlList.add((String) parameters[0]);
		}
		else if (method.equals(Statement.class.getMethod("clearBatch")))
		{
			this.sqlList.clear();
		}
		else if (method.equals(Statement.class.getMethod("close")))
		{
			this.getParentProxy().removeChild(this);
		}
	}

	/**
	 * @see net.sf.hajdbc.sql.SQLProxy#handlePartialFailure(java.util.SortedMap, java.util.SortedMap)
	 */
	@Override
	public <R> SortedMap<Database<D>, R> handlePartialFailure(SortedMap<Database<D>, R> resultMap, SortedMap<Database<D>, SQLException> exceptionMap) throws SQLException
	{
		if (this.getParent().getAutoCommit())
		{
			return super.handlePartialFailure(resultMap, exceptionMap);
		}
		
		// If auto-commit is off, throw exception to give client the opportunity to rollback the transaction
		DatabaseCluster<D> cluster = this.getDatabaseCluster();
		
		Map<Database<D>, Boolean> aliveMap = cluster.getAliveMap(exceptionMap.keySet());

		SQLException exception = null;
		
		for (Map.Entry<Database<D>, SQLException> exceptionMapEntry: exceptionMap.entrySet())
		{
			Database<D> database = exceptionMapEntry.getKey();
			SQLException cause = exceptionMapEntry.getValue();
			
			if (aliveMap.get(database))
			{
				if (exception == null)
				{
					exception = cause;
				}
				else
				{
					exception.setNextException(cause);
				}
			}
			else
			{
				if (cluster.deactivate(database, cluster.getStateManager()))
				{
					this.logger.error(Messages.getMessage(Messages.DATABASE_DEACTIVATED, database, cluster), cause);
				}
			}
		}
		
		if (exception != null)
		{
			throw exception;
		}

		return resultMap;
	}
	
	protected boolean isSelectForUpdate(String sql) throws SQLException
	{
		DatabaseCluster<D> databaseCluster = this.getDatabaseCluster();
		
		return databaseCluster.getDatabaseMetaDataCache().getDatabaseProperties(this.getParent()).supportsSelectForUpdate() ? databaseCluster.getDialect().isSelectForUpdate(sql) : false;
	}
	
	protected List<Lock> getLockList(String sql) throws SQLException
	{
		return this.getLockList(Collections.singletonList(sql));
	}
	
	private List<Lock> getLockList(List<String> sqlList) throws SQLException
	{
		DatabaseCluster<D> databaseCluster = this.getDatabaseCluster();
		
		Dialect dialect = databaseCluster.getDialect();
		
		Set<String> identifierSet = new LinkedHashSet<String>(sqlList.size());
		
		for (String sql: sqlList)
		{
			if (databaseCluster.isSequenceDetectionEnabled())
			{
				String sequence = dialect.parseSequence(sql);
				
				if (sequence != null)
				{
					identifierSet.add(sequence);
				}
			}
			
			if (databaseCluster.isIdentityColumnDetectionEnabled())
			{
				String table = dialect.parseInsertTable(sql);
				
				if (table != null)
				{
					TableProperties tableProperties = databaseCluster.getDatabaseMetaDataCache().getDatabaseProperties(this.getParent()).findTable(table);
					
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
			LockManager lockManager = databaseCluster.getLockManager();
			
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
}
