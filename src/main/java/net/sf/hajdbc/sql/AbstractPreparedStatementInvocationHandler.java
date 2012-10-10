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

import java.io.File;
import java.io.InputStream;
import java.io.Reader;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.Lock;

import javax.sql.rowset.serial.SerialBlob;
import javax.sql.rowset.serial.SerialClob;

import net.sf.hajdbc.Database;
import net.sf.hajdbc.invocation.InvocationStrategy;
import net.sf.hajdbc.invocation.InvocationStrategyEnum;
import net.sf.hajdbc.invocation.Invoker;
import net.sf.hajdbc.util.reflect.Methods;
import net.sf.hajdbc.util.reflect.ProxyFactory;
import net.sf.hajdbc.util.reflect.SimpleInvocationHandler;

/**
 * @author Paul Ferraro
 * @param <D> 
 * @param <S> 
 */
@SuppressWarnings("nls")
public abstract class AbstractPreparedStatementInvocationHandler<Z, D extends Database<Z>, S extends PreparedStatement> extends AbstractStatementInvocationHandler<Z, D, S>
{
	private static final Set<Method> databaseReadMethodSet = Methods.findMethods(PreparedStatement.class, "getMetaData", "getParameterMetaData");
	private static final Method executeMethod = Methods.getMethod(PreparedStatement.class, "execute");
	private static final Method executeUpdateMethod = Methods.getMethod(PreparedStatement.class, "executeUpdate");
	private static final Method executeQueryMethod = Methods.getMethod(PreparedStatement.class, "executeQuery");
	private static final Method clearParametersMethod = Methods.getMethod(PreparedStatement.class, "clearParameters");
	private static final Method addBatchMethod = Methods.getMethod(PreparedStatement.class, "addBatch");
	
	protected List<Lock> lockList = Collections.emptyList();
	protected boolean selectForUpdate = false;
	private final Set<Method> setMethodSet;
	
	/**
	 * @param connection
	 * @param proxy
	 * @param invoker
	 * @param statementClass 
	 * @param statementMap
	 * @param transactionContext 
	 * @param fileSupport 
	 * @throws Exception
	 */
	protected AbstractPreparedStatementInvocationHandler(Connection connection, SQLProxy<Z, D, Connection, SQLException> proxy, Invoker<Z, D, Connection, S, SQLException> invoker, Class<S> statementClass, Map<D, S> statementMap, TransactionContext<Z, D> transactionContext, FileSupport<SQLException> fileSupport, Set<Method> setMethodSet)
	{
		super(connection, proxy, invoker, statementClass, statementMap, transactionContext, fileSupport);
		
		this.setMethodSet = setMethodSet;
	}
	
	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.sql.AbstractStatementInvocationHandler#getInvocationHandlerFactory(java.sql.Statement, java.lang.reflect.Method, java.lang.Object[])
	 */
	@Override
	protected InvocationHandlerFactory<Z, D, S, ?, SQLException> getInvocationHandlerFactory(S object, Method method, Object[] parameters) throws SQLException
	{
		if (method.equals(executeQueryMethod))
		{
			return new ResultSetInvocationHandlerFactory<Z, D, S>(this.transactionContext, this.fileSupport);
		}
		
		return super.getInvocationHandlerFactory(object, method, parameters);
	}

	/**
	 * @see net.sf.hajdbc.sql.AbstractStatementInvocationHandler#getInvocationStrategy(java.sql.Statement, java.lang.reflect.Method, java.lang.Object[])
	 */
	@Override
	protected InvocationStrategy getInvocationStrategy(S statement, Method method, Object[] parameters) throws SQLException
	{
		if (databaseReadMethodSet.contains(method))
		{
			return InvocationStrategyEnum.INVOKE_ON_NEXT;
		}
		
		if (this.setMethodSet.contains(method) || method.equals(clearParametersMethod) || method.equals(addBatchMethod))
		{
			return InvocationStrategyEnum.INVOKE_ON_EXISTING;
		}
		
		if (method.equals(executeMethod) || method.equals(executeUpdateMethod))
		{
			return this.transactionContext.start(new LockingInvocationStrategy(InvocationStrategyEnum.TRANSACTION_INVOKE_ON_ALL, this.lockList), this.getParent());
		}
		
		if (method.equals(executeQueryMethod))
		{
			int concurrency = statement.getResultSetConcurrency();
			
			if (this.lockList.isEmpty() && (concurrency == ResultSet.CONCUR_READ_ONLY) && !this.selectForUpdate)
			{
				return InvocationStrategyEnum.INVOKE_ON_NEXT;
			}
			
			InvocationStrategy strategy = new LockingInvocationStrategy(InvocationStrategyEnum.TRANSACTION_INVOKE_ON_ALL, this.lockList);
			
			return this.selectForUpdate ? this.transactionContext.start(strategy, this.getParent()) : strategy;
		}
		
		return super.getInvocationStrategy(statement, method, parameters);
	}

	/**
	 * @see net.sf.hajdbc.sql.AbstractChildInvocationHandler#getInvoker(java.lang.Object, java.lang.reflect.Method, java.lang.Object[])
	 */
	@Override
	protected <R> Invoker<Z, D, S, R, SQLException> getInvoker(S statement, final Method method, final Object[] parameters) throws SQLException
	{
		if (this.isParameterSetMethod(method) && (parameters.length > 1))
		{
			Object typeParameter = parameters[1];
			
			if (typeParameter != null)
			{
				Class<?> type = method.getParameterTypes()[1];
				
				if (type.equals(InputStream.class))
				{
					final File file = this.fileSupport.createFile((InputStream) typeParameter);
					
					return new Invoker<Z, D, S, R, SQLException>()
					{
						@Override
						public R invoke(D database, S statement) throws SQLException
						{
							List<Object> parameterList = new ArrayList<Object>(Arrays.asList(parameters));
							
							parameterList.set(1, AbstractPreparedStatementInvocationHandler.this.fileSupport.getInputStream(file));
							
							return Methods.<R, SQLException>invoke(method, AbstractPreparedStatementInvocationHandler.this.getExceptionFactory(), statement, parameterList.toArray());
						}				
					};
				}
				
				if (type.equals(Reader.class))
				{
					final File file = this.fileSupport.createFile((Reader) typeParameter);
					
					return new Invoker<Z, D, S, R, SQLException>()
					{
						@Override
						public R invoke(D database, S statement) throws SQLException
						{
							List<Object> parameterList = new ArrayList<Object>(Arrays.asList(parameters));
							
							parameterList.set(1, AbstractPreparedStatementInvocationHandler.this.fileSupport.getReader(file));
							
							return Methods.<R, SQLException>invoke(method, AbstractPreparedStatementInvocationHandler.this.getExceptionFactory(), statement, parameterList.toArray());
						}				
					};
				}
				
				if (type.equals(Blob.class))
				{
					Blob blob = (Blob) typeParameter;
					
					if (Proxy.isProxyClass(blob.getClass()) && (Proxy.getInvocationHandler(blob) instanceof SQLProxy<?, ?, ?, ?>))
					{
						final SQLProxy<Z, D, Blob, SQLException> proxy = this.getInvocationHandler(blob);
						
						return new Invoker<Z, D, S, R, SQLException>()
						{
							@Override
							public R invoke(D database, S statement) throws SQLException
							{
								List<Object> parameterList = new ArrayList<Object>(Arrays.asList(parameters));
								
								parameterList.set(1, proxy.getObject(database));
								
								return Methods.<R, SQLException>invoke(method, AbstractPreparedStatementInvocationHandler.this.getExceptionFactory(), statement, parameterList.toArray());
							}				
						};
					}

					parameters[1] = new SerialBlob(blob);
				}
				
				// Handle both clob and nclob
				if (Clob.class.isAssignableFrom(type))
				{
					Clob clob = (Clob) typeParameter;
					
					if (Proxy.isProxyClass(clob.getClass()) && (Proxy.getInvocationHandler(clob) instanceof SQLProxy<?, ?, ?, ?>))
					{
						final SQLProxy<Z, D, Clob, SQLException> proxy = this.getInvocationHandler(clob);
						
						return new Invoker<Z, D, S, R, SQLException>()
						{
							@Override
							public R invoke(D database, S statement) throws SQLException
							{
								List<Object> parameterList = new ArrayList<Object>(Arrays.asList(parameters));
								
								parameterList.set(1, proxy.getObject(database));
								
								return Methods.<R, SQLException>invoke(method, AbstractPreparedStatementInvocationHandler.this.getExceptionFactory(), statement, parameterList.toArray());
							}				
						};
					}

					Clob serialClob = new SerialClob(clob);
					
					parameters[1] = type.equals(Clob.class) ? serialClob : ProxyFactory.createProxy(type, new SimpleInvocationHandler(serialClob));
				}
			}
		}
		
		return super.getInvoker(statement, method, parameters);
	}
	
	/**
	 * @see net.sf.hajdbc.sql.AbstractStatementInvocationHandler#isBatchMethod(java.lang.reflect.Method)
	 */
	@Override
	protected boolean isBatchMethod(Method method)
	{
		return method.equals(addBatchMethod) || method.equals(clearParametersMethod) || this.isParameterSetMethod(method) || super.isBatchMethod(method);
	}

	private boolean isParameterSetMethod(Method method)
	{
		Class<?>[] types = method.getParameterTypes();
		
		return this.setMethodSet.contains(method) && (types.length > 0) && this.isIndexType(types[0]);
	}
	
	protected boolean isIndexType(Class<?> type)
	{
		return type.equals(Integer.TYPE);
	}
}
