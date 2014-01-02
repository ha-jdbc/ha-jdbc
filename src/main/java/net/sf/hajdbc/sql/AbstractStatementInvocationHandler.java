/*
 * HA-JDBC: High-Availability JDBC
 * Copyright (C) 2012  Paul Ferraro
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package net.sf.hajdbc.sql;

import java.io.IOException;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Set;
import java.util.concurrent.locks.Lock;

import net.sf.hajdbc.Database;
import net.sf.hajdbc.invocation.InvocationStrategies;
import net.sf.hajdbc.invocation.InvocationStrategy;
import net.sf.hajdbc.invocation.Invoker;
import net.sf.hajdbc.invocation.LockingInvocationStrategy;
import net.sf.hajdbc.logging.Level;
import net.sf.hajdbc.util.reflect.Methods;

/**
 * @author Paul Ferraro
 * @param <D> 
 * @param <S> 
 */
@SuppressWarnings("nls")
public abstract class AbstractStatementInvocationHandler<Z, D extends Database<Z>, S extends Statement, F extends AbstractStatementProxyFactory<Z, D, S>> extends InputSinkRegistryInvocationHandler<Z, D, Connection, S, F>
{
	private static final Set<Method> driverReadMethodSet = Methods.findMethods(Statement.class, "getFetchDirection", "getFetchSize", "getGeneratedKeys", "getMaxFieldSize", "getMaxRows", "getQueryTimeout", "getResultSetConcurrency", "getResultSetHoldability", "getResultSetType", "getUpdateCount", "getWarnings", "isClosed", "isPoolable");
	private static final Set<Method> driverWriteMethodSet = Methods.findMethods(Statement.class, "clearWarnings", "setCursorName", "setEscapeProcessing", "setFetchDirection", "setFetchSize", "setMaxFieldSize", "setMaxRows", "setPoolable", "setQueryTimeout");
	private static final Set<Method> executeMethodSet = Methods.findMethods(Statement.class, "execute(Update)?");
	
	private static final Method getConnectionMethod = Methods.getMethod(Statement.class, "getConnection");
	private static final Method executeQueryMethod = Methods.getMethod(Statement.class, "executeQuery", String.class);
	private static final Method clearBatchMethod = Methods.getMethod(Statement.class, "clearBatch");
	private static final Method executeBatchMethod = Methods.getMethod(Statement.class, "executeBatch");
	private static final Method getMoreResultsMethod = Methods.getMethod(Statement.class, "getMoreResults", Integer.TYPE);
	private static final Method getResultSetMethod = Methods.getMethod(Statement.class, "getResultSet");
	private static final Method addBatchMethod = Methods.getMethod(Statement.class, "addBatch", String.class);
	private static final Method closeMethod = Methods.getMethod(Statement.class, "close");
	
	public AbstractStatementInvocationHandler(Class<S> statementClass, F proxyFactory)
	{
		super(statementClass, proxyFactory, getConnectionMethod);
	}

	@Override
	protected ProxyFactoryFactory<Z, D, S, SQLException, ?, ? extends Exception> getProxyFactoryFactory(S object, Method method, Object... parameters) throws SQLException
	{
		if (method.equals(executeQueryMethod) || method.equals(getResultSetMethod))
		{
			return new ResultSetProxyFactoryFactory<>(this.getProxyFactory().getTransactionContext(), this.getProxyFactory().getInputSinkRegistry());
		}
		
		return super.getProxyFactoryFactory(object, method, parameters);
	}

	@Override
	protected InvocationStrategy getInvocationStrategy(S statement, Method method, Object... parameters) throws SQLException
	{
		if (driverReadMethodSet.contains(method))
		{
			return InvocationStrategies.INVOKE_ON_ANY;
		}
		
		if (driverWriteMethodSet.contains(method) || method.equals(closeMethod))
		{
			return InvocationStrategies.INVOKE_ON_EXISTING;
		}
		
		if (executeMethodSet.contains(method))
		{
			List<Lock> locks = this.getProxyFactory().extractLocks((String) parameters[0]);
			
			return this.getProxyFactory().getTransactionContext().start(new LockingInvocationStrategy(InvocationStrategies.TRANSACTION_INVOKE_ON_ALL, locks), this.getProxyFactory().getParentProxy());
		}
		
		if (method.equals(executeQueryMethod))
		{
			String sql = (String) parameters[0];
			
			List<Lock> locks = this.getProxyFactory().extractLocks(sql);
			int concurrency = statement.getResultSetConcurrency();
			boolean selectForUpdate = this.getProxyFactory().isSelectForUpdate(sql);
			
			if (locks.isEmpty() && (concurrency == ResultSet.CONCUR_READ_ONLY) && !selectForUpdate)
			{
				boolean repeatableReadSelect = (statement.getConnection().getTransactionIsolation() >= Connection.TRANSACTION_REPEATABLE_READ);
				
				return repeatableReadSelect ? InvocationStrategies.INVOKE_ON_PRIMARY : InvocationStrategies.INVOKE_ON_NEXT;
			}
			
			InvocationStrategy strategy = InvocationStrategies.TRANSACTION_INVOKE_ON_ALL;
			if (!locks.isEmpty())
			{
				strategy = new LockingInvocationStrategy(strategy, locks);
			}
			
			return selectForUpdate ? this.getProxyFactory().getTransactionContext().start(strategy, this.getProxyFactory().getParentProxy()) : strategy;
		}
		
		if (method.equals(executeBatchMethod))
		{
			return this.getProxyFactory().getTransactionContext().start(new LockingInvocationStrategy(InvocationStrategies.TRANSACTION_INVOKE_ON_ALL, this.getProxyFactory().getBatchLocks()), this.getProxyFactory().getParentProxy());
		}
		
		if (method.equals(getMoreResultsMethod))
		{
			if (parameters[0].equals(Statement.KEEP_CURRENT_RESULT))
			{
				return InvocationStrategies.INVOKE_ON_EXISTING;
			}
		}
		
		if (method.equals(getResultSetMethod))
		{
			if (statement.getResultSetConcurrency() == ResultSet.CONCUR_READ_ONLY)
			{
				return InvocationStrategies.INVOKE_ON_EXISTING;
			}

			return InvocationStrategies.INVOKE_ON_ALL;
		}
		
		return super.getInvocationStrategy(statement, method, parameters);
	}

	@Override
	protected <R> Invoker<Z, D, S, R, SQLException> getInvoker(S proxy, Method method, Object... parameters) throws SQLException
	{
		if (method.equals(addBatchMethod) || method.equals(executeQueryMethod) || executeMethodSet.contains(method))
		{
			parameters[0] = this.getProxyFactory().evaluate((String) parameters[0]);
		}
		
		return super.getInvoker(proxy, method, parameters);
	}

	@Override
	protected <R> void postInvoke(Invoker<Z, D, S, R, SQLException> invoker, S proxy, Method method, Object... parameters)
	{
		if (method.equals(addBatchMethod))
		{
			this.getProxyFactory().addBatchSQL((String) parameters[0]);
		}
		else if (method.equals(clearBatchMethod) || method.equals(executeBatchMethod))
		{
			this.getProxyFactory().clearBatch();
			this.logger.log(Level.TRACE, "Clearing recorded batch methods");
			this.getProxyFactory().clearBatchInvokers();
		}
		else if (method.equals(closeMethod))
		{
			try
			{
				this.getProxyFactory().getInputSinkRegistry().close();
			}
			catch (IOException e)
			{
				this.logger.log(Level.WARN, e);
			}
			this.getProxyFactory().remove();
		}
		
		if (this.isBatchMethod(method))
		{
			this.logger.log(Level.TRACE, "Recording batch method: {0}", invoker);
			this.getProxyFactory().addBatchInvoker(invoker);
		}
		else if (driverWriteMethodSet.contains(method))
		{
			this.getProxyFactory().record(invoker);
		}
	}

	protected boolean isBatchMethod(Method method)
	{
		return method.equals(addBatchMethod);
	}
}
