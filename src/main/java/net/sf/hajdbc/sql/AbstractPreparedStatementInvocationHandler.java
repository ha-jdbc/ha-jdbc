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
	 * @see net.sf.hajdbc.sql.AbstractStatementInvocationHandler#getInvocationStrategy(java.sql.Statement, java.lang.reflect.Method, java.lang.Object[])
	 */
	@Override
	protected InvocationStrategy<Z, D, S, ?, SQLException> getInvocationStrategy(S statement, Method method, Object[] parameters) throws SQLException
	{
		if (databaseReadMethodSet.contains(method))
		{
			return new DatabaseReadInvocationStrategy<Z, D, S, Object, SQLException>();
		}
		
		if (this.setMethodSet.contains(method))
		{
			return new DriverWriteInvocationStrategy<Z, D, S, Object, SQLException>();
		}
		
		if (method.equals(executeMethod) || method.equals(executeUpdateMethod))
		{
			return this.transactionContext.start(new LockingInvocationStrategy<Z, D, S, Object, SQLException>(new DatabaseWriteInvocationStrategy<Z, D, S, Object, SQLException>(this.cluster.getTransactionalExecutor()), this.lockList), this.getParent());
		}
		
		if (method.equals(executeQueryMethod))
		{
			int concurrency = statement.getResultSetConcurrency();
			
			if (this.lockList.isEmpty() && (concurrency == ResultSet.CONCUR_READ_ONLY) && !this.selectForUpdate)
			{
				return new DatabaseReadInvocationStrategy<Z, D, S, Object, SQLException>();
			}
			
			InvocationStrategy<Z, D, S, ResultSet, SQLException> strategy = new LockingInvocationStrategy<Z, D, S, ResultSet, SQLException>(new EagerResultSetInvocationStrategy<Z, D, S>(this.cluster, statement, this.transactionContext, this.fileSupport), this.lockList);
			
			return this.selectForUpdate ? this.transactionContext.start(strategy, this.getParent()) : strategy;
		}
		
		return super.getInvocationStrategy(statement, method, parameters);
	}

	/**
	 * @see net.sf.hajdbc.sql.AbstractChildInvocationHandler#getInvoker(java.lang.Object, java.lang.reflect.Method, java.lang.Object[])
	 */
	@Override
	protected Invoker<Z, D, S, ?, SQLException> getInvoker(S statement, final Method method, final Object[] parameters) throws SQLException
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
					
					return new Invoker<Z, D, S, Object, SQLException>()
					{
						public Object invoke(D database, S statement) throws SQLException
						{
							List<Object> parameterList = new ArrayList<Object>(Arrays.asList(parameters));
							
							parameterList.set(1, AbstractPreparedStatementInvocationHandler.this.fileSupport.getInputStream(file));
							
							return Methods.invoke(method, AbstractPreparedStatementInvocationHandler.this.getExceptionFactory(), statement, parameterList.toArray());
						}				
					};
				}
				
				if (type.equals(Reader.class))
				{
					final File file = this.fileSupport.createFile((Reader) typeParameter);
					
					return new Invoker<Z, D, S, Object, SQLException>()
					{
						public Object invoke(D database, S statement) throws SQLException
						{
							List<Object> parameterList = new ArrayList<Object>(Arrays.asList(parameters));
							
							parameterList.set(1, AbstractPreparedStatementInvocationHandler.this.fileSupport.getReader(file));
							
							return Methods.invoke(method, AbstractPreparedStatementInvocationHandler.this.getExceptionFactory(), statement, parameterList.toArray());
						}				
					};
				}
				
				if (type.equals(Blob.class))
				{
					Blob blob = (Blob) typeParameter;
					
					if (Proxy.isProxyClass(blob.getClass()) && (Proxy.getInvocationHandler(blob) instanceof SQLProxy<?, ?, ?, ?>))
					{
						final SQLProxy<Z, D, Blob, SQLException> proxy = this.getInvocationHandler(blob);
						
						return new Invoker<Z, D, S, Object, SQLException>()
						{
							public Object invoke(D database, S statement) throws SQLException
							{
								List<Object> parameterList = new ArrayList<Object>(Arrays.asList(parameters));
								
								parameterList.set(1, proxy.getObject(database));
								
								return Methods.invoke(method, AbstractPreparedStatementInvocationHandler.this.getExceptionFactory(), statement, parameterList.toArray());
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
						
						return new Invoker<Z, D, S, Object, SQLException>()
						{
							public Object invoke(D database, S statement) throws SQLException
							{
								List<Object> parameterList = new ArrayList<Object>(Arrays.asList(parameters));
								
								parameterList.set(1, proxy.getObject(database));
								
								return Methods.invoke(method, AbstractPreparedStatementInvocationHandler.this.getExceptionFactory(), statement, parameterList.toArray());
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
