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

import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Set;
import java.util.concurrent.locks.Lock;

import net.sf.hajdbc.Database;
import net.sf.hajdbc.invocation.InvocationStrategies;
import net.sf.hajdbc.invocation.InvocationStrategy;
import net.sf.hajdbc.invocation.Invoker;
import net.sf.hajdbc.invocation.LockingInvocationStrategy;
import net.sf.hajdbc.util.reflect.Methods;

/**
 * @author Paul Ferraro
 * @param <D> 
 * @param <S> 
 */
public abstract class AbstractPreparedStatementInvocationHandler<Z, D extends Database<Z>, S extends PreparedStatement, F extends AbstractPreparedStatementProxyFactory<Z, D, S>> extends AbstractStatementInvocationHandler<Z, D, S, F>
{
	private static final Set<Method> databaseReadMethodSet = Methods.findMethods(PreparedStatement.class, "getMetaData", "getParameterMetaData");
	private static final Method executeMethod = Methods.getMethod(PreparedStatement.class, "execute");
	private static final Method executeUpdateMethod = Methods.getMethod(PreparedStatement.class, "executeUpdate");
	private static final Method executeQueryMethod = Methods.getMethod(PreparedStatement.class, "executeQuery");
	private static final Method clearParametersMethod = Methods.getMethod(PreparedStatement.class, "clearParameters");
	private static final Method addBatchMethod = Methods.getMethod(PreparedStatement.class, "addBatch");
	
	private final Set<Method> setMethods;
	
	public AbstractPreparedStatementInvocationHandler(Class<S> statementClass, F proxyFactory, Set<Method> setMethods)
	{
		super(statementClass, proxyFactory);
		this.setMethods = setMethods;
	}
	
	@Override
	protected ProxyFactoryFactory<Z, D, S, SQLException, ?, ? extends Exception> getProxyFactoryFactory(S object, Method method, Object... parameters) throws SQLException
	{
		if (method.equals(executeQueryMethod))
		{
			return new ResultSetProxyFactoryFactory<>(this.getProxyFactory().getTransactionContext(), this.getProxyFactory().getInputSinkRegistry());
		}
		
		return super.getProxyFactoryFactory(object, method, parameters);
	}

	/**
	 * @see net.sf.hajdbc.sql.AbstractStatementInvocationHandler#getInvocationStrategy(java.sql.Statement, java.lang.reflect.Method, java.lang.Object[])
	 */
	@Override
	protected InvocationStrategy getInvocationStrategy(S statement, Method method, Object... parameters) throws SQLException
	{
		if (databaseReadMethodSet.contains(method))
		{
			return InvocationStrategies.INVOKE_ON_NEXT;
		}
		
		if (this.setMethods.contains(method) || method.equals(clearParametersMethod) || method.equals(addBatchMethod))
		{
			return InvocationStrategies.INVOKE_ON_EXISTING;
		}
		
		if (method.equals(executeMethod) || method.equals(executeUpdateMethod))
		{
			return this.getProxyFactory().getTransactionContext().start(new LockingInvocationStrategy(InvocationStrategies.TRANSACTION_INVOKE_ON_ALL, this.getProxyFactory().getLocks()), this.getProxyFactory().getParentProxy());
		}
		
		if (method.equals(executeQueryMethod))
		{
			List<Lock> locks = this.getProxyFactory().getLocks();
			int concurrency = statement.getResultSetConcurrency();
			boolean selectForUpdate = this.getProxyFactory().isSelectForUpdate();
			
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
		
		return super.getInvocationStrategy(statement, method, parameters);
	}

	@Override
	protected <R> Invoker<Z, D, S, R, SQLException> getInvoker(S statement, final Method method, final Object... parameters) throws SQLException
	{
		if (this.isSetParameterMethod(method) && (parameters.length > 1))
		{
			return this.getInvoker(method.getParameterTypes()[1], 1, statement, method, parameters);
		}
		
		return super.getInvoker(statement, method, parameters);
	}
	
	@Override
	protected boolean isBatchMethod(Method method)
	{
		return method.equals(addBatchMethod) || method.equals(clearParametersMethod) || this.isSetParameterMethod(method) || super.isBatchMethod(method);
	}

	private boolean isSetParameterMethod(Method method)
	{
		Class<?>[] types = method.getParameterTypes();
		
		return this.setMethods.contains(method) && (types.length > 0) && this.isIndexType(types[0]);
	}
	
	protected boolean isIndexType(Class<?> type)
	{
		return type.equals(Integer.TYPE);
	}
}
