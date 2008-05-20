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
import java.lang.reflect.InvocationHandler;
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
public class AbstractPreparedStatementInvocationHandler<D, S extends PreparedStatement> extends AbstractStatementInvocationHandler<D, S>
{
	private static final Set<Method> databaseReadMethodSet = Methods.findMethods(PreparedStatement.class, "getMetaData", "getParameterMetaData");
	private static final Set<Method> driverWriteMethodSet = Methods.findMethods(PreparedStatement.class, "addBatch", "clearParameters", "set\\w+");
	private static final Method executeMethod = Methods.getMethod(PreparedStatement.class, "execute");
	private static final Method executeUpdateMethod = Methods.getMethod(PreparedStatement.class, "executeUpdate");
	private static final Method executeQueryMethod = Methods.getMethod(PreparedStatement.class, "executeQuery");
	private static final Set<Method> recordableMethodSet = Methods.findMethods(PreparedStatement.class, "addBatch", "clearParameters");
	
	protected List<Lock> lockList = Collections.emptyList();
	protected boolean selectForUpdate = false;
	
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
	public AbstractPreparedStatementInvocationHandler(Connection connection, SQLProxy<D, Connection> proxy, Invoker<D, Connection, S> invoker, Class<S> statementClass, Map<Database<D>, S> statementMap, TransactionContext<D> transactionContext, FileSupport fileSupport) throws Exception
	{
		super(connection, proxy, invoker, statementClass, statementMap, transactionContext, fileSupport);
	}
	
	/**
	 * @see net.sf.hajdbc.sql.AbstractStatementInvocationHandler#getInvocationStrategy(java.sql.Statement, java.lang.reflect.Method, java.lang.Object[])
	 */
	@Override
	protected InvocationStrategy<D, S, ?> getInvocationStrategy(S statement, Method method, Object[] parameters) throws Exception
	{
		if (databaseReadMethodSet.contains(method))
		{
			return new DatabaseReadInvocationStrategy<D, S, Object>();
		}
		
		if (driverWriteMethodSet.contains(method))
		{
			return new DriverWriteInvocationStrategy<D, S, Object>();
		}
		
		if (method.equals(executeMethod) || method.equals(executeUpdateMethod))
		{
			return this.transactionContext.start(new LockingInvocationStrategy<D, S, Object>(new DatabaseWriteInvocationStrategy<D, S, Object>(this.cluster.getTransactionalExecutor()), this.lockList), this.getParent());
		}
		
		if (method.equals(executeQueryMethod))
		{
			int concurrency = statement.getResultSetConcurrency();
			
			if (this.lockList.isEmpty() && (concurrency == ResultSet.CONCUR_READ_ONLY) && !this.selectForUpdate)
			{
				return new DatabaseReadInvocationStrategy<D, S, Object>();
			}
			
			InvocationStrategy<D, S, ResultSet> strategy = new LockingInvocationStrategy<D, S, ResultSet>(new EagerResultSetInvocationStrategy<D, S>(this.cluster, statement, this.transactionContext, this.fileSupport), this.lockList);
			
			return this.selectForUpdate ? this.transactionContext.start(strategy, this.getParent()) : strategy;
		}
		
		return super.getInvocationStrategy(statement, method, parameters);
	}

	/**
	 * @see net.sf.hajdbc.sql.AbstractChildInvocationHandler#getInvoker(java.lang.Object, java.lang.reflect.Method, java.lang.Object[])
	 */
	@SuppressWarnings("unchecked")
	@Override
	protected Invoker<D, S, ?> getInvoker(S statement, final Method method, final Object[] parameters) throws Exception
	{
		Class<?>[] types = method.getParameterTypes();
		
		if (this.isIndexSetMethod(method))
		{
			Class<?> type = types[1];
			
			if (type.equals(InputStream.class))
			{
				final File file = this.fileSupport.createFile((InputStream) parameters[1]);
				
				return new Invoker<D, S, Object>()
				{
					public Object invoke(Database<D> database, S statement) throws SQLException
					{
						List<Object> parameterList = new ArrayList<Object>(Arrays.asList(parameters));
						
						parameterList.set(1, AbstractPreparedStatementInvocationHandler.this.fileSupport.getInputStream(file));
						
						return Methods.invoke(method, statement, parameterList.toArray());
					}				
				};
			}
			
			if (type.equals(Reader.class))
			{
				final File file = this.fileSupport.createFile((Reader) parameters[1]);
				
				return new Invoker<D, S, Object>()
				{
					public Object invoke(Database<D> database, S statement) throws SQLException
					{
						List<Object> parameterList = new ArrayList<Object>(Arrays.asList(parameters));
						
						parameterList.set(1, AbstractPreparedStatementInvocationHandler.this.fileSupport.getReader(file));
						
						return Methods.invoke(method, statement, parameterList.toArray());
					}				
				};
			}
			
			if (type.equals(Blob.class))
			{
				Blob blob = (Blob) parameters[1];
				
				if (Proxy.isProxyClass(blob.getClass()))
				{
					InvocationHandler handler = Proxy.getInvocationHandler(blob);
					
					if (BlobInvocationHandler.class.isInstance(handler))
					{
						final BlobInvocationHandler<D, ?> proxy = (BlobInvocationHandler) handler;
						
						return new Invoker<D, S, Object>()
						{
							public Object invoke(Database<D> database, S statement) throws SQLException
							{
								List<Object> parameterList = new ArrayList<Object>(Arrays.asList(parameters));
								
								parameterList.set(1, proxy.getObject(database));
								
								return Methods.invoke(method, statement, parameterList.toArray());
							}				
						};
					}
				}

				parameters[1] = new SerialBlob(blob);
			}
		
			// Handle both clob and nclob
			if (Clob.class.isAssignableFrom(type))
			{
				Clob clob = (Clob) parameters[1];
				
				if (Proxy.isProxyClass(clob.getClass()))
				{
					InvocationHandler handler = Proxy.getInvocationHandler(clob);
					
					if (ClobInvocationHandler.class.isInstance(handler))
					{
						final ClobInvocationHandler<D, ?> proxy = (ClobInvocationHandler) handler;
						
						return new Invoker<D, S, Object>()
						{
							public Object invoke(Database<D> database, S statement) throws SQLException
							{
								List<Object> parameterList = new ArrayList<Object>(Arrays.asList(parameters));
								
								parameterList.set(1, proxy.getObject(database));
								
								return Methods.invoke(method, statement, parameterList.toArray());
							}				
						};
					}
				}

				Clob serialClob = new SerialClob(clob);
				
				parameters[1] = type.equals(Clob.class) ? serialClob : ProxyFactory.createProxy(type, new SimpleInvocationHandler(serialClob));
			}
		}
		
		return super.getInvoker(statement, method, parameters);
	}

	private boolean isIndexSetMethod(Method method)
	{
		Class<?>[] types = method.getParameterTypes();
		
		return this.isSetMethod(method) && (types.length > 1) && this.isIndexType(types[0]);
	}
	
	protected boolean isIndexType(Class<?> type)
	{
		return type.equals(Integer.TYPE);
	}
	
	/**
	 * @see net.sf.hajdbc.sql.AbstractStatementInvocationHandler#isRecordable(java.lang.reflect.Method)
	 */
	@Override
	protected boolean isRecordable(Method method)
	{
		return super.isRecordable(method) || recordableMethodSet.contains(method) || this.isIndexSetMethod(method);
	}
}
