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
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;

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
public class ResultSetInvocationHandler<D, S extends Statement> extends AbstractChildInvocationHandler<D, S, ResultSet>
{
	private static final Set<Method> driverReadMethodSet = Methods.findMethods(ResultSet.class, "findColumn", "getConcurrency", "getCursorName", "getFetchDirection", "getFetchSize", "getHoldability", "getMetaData", "getRow", "getType", "getWarnings", "isAfterLast", "isBeforeFirst", "isClosed", "isFirst", "isLast", "row(Deleted|Inserted|Updated)", "wasNull");
	private static final Set<Method> driverWriteMethodSet = Methods.findMethods(ResultSet.class, "clearWarnings", "setFetchDirection", "setFetchSize");
	private static final Set<Method> absoluteNavigationMethodSet = Methods.findMethods(ResultSet.class, "absolute", "afterLast", "beforeFirst", "first", "last");
	private static final Set<Method> relativeNavigationMethodSet = Methods.findMethods(ResultSet.class, "moveTo(Current|Insert)Row", "next", "previous", "relative");
	private static final Set<Method> transactionalWriteMethodSet = Methods.findMethods(ResultSet.class, "(delete|insert|update)Row");
	
	private static final Method closeMethod = Methods.getMethod(ResultSet.class, "close");
	private static final Method cancelRowUpdatesMethod = Methods.getMethod(ResultSet.class, "cancelRowUpdates");
	private static final Method getStatementMethod = Methods.getMethod(ResultSet.class, "getStatement");
	
	private static final Set<Method> getBlobMethodSet = Methods.findMethods(ResultSet.class, "getBlob");
	private static final Set<Method> getClobMethodSet = Methods.findMethods(ResultSet.class, "getClob", "getNClob");
	
	protected FileSupport fileSupport;
	private TransactionContext<D> transactionContext;
	private List<Invoker<D, ResultSet, ?>> updateInvokerList = new LinkedList<Invoker<D, ResultSet, ?>>();
	private Invoker<D, ResultSet, ?> absoluteNavigationInvoker = null;
	private List<Invoker<D, ResultSet, ?>> relativeNavigationInvokerList = new LinkedList<Invoker<D, ResultSet, ?>>();
	
	/**
	 * @param statement the statement that created this result set
	 * @param proxy the invocation handler of the statement that created this result set
	 * @param invoker the invoker that was used to create this result set
	 * @param resultSetMap a map of database to underlying result set
	 * @param transactionContext 
	 * @param fileSupport support for streams
	 * @throws Exception
	 */
	protected ResultSetInvocationHandler(S statement, SQLProxy<D, S> proxy, Invoker<D, S, ResultSet> invoker, Map<Database<D>, ResultSet> resultSetMap, TransactionContext<D> transactionContext, FileSupport fileSupport) throws Exception
	{
		super(statement, proxy, invoker, ResultSet.class, resultSetMap);
		
		this.transactionContext = transactionContext;
		this.fileSupport = fileSupport;
	}

	/**
	 * @see net.sf.hajdbc.sql.AbstractChildInvocationHandler#getInvocationStrategy(java.lang.Object, java.lang.reflect.Method, java.lang.Object[])
	 */
	@Override
	protected InvocationStrategy<D, ResultSet, ?> getInvocationStrategy(ResultSet resultSet, Method method, Object[] parameters) throws Exception
	{
		if (driverReadMethodSet.contains(method))
		{
			return new DriverReadInvocationStrategy<D, ResultSet, Object>();
		}
		
		if (driverWriteMethodSet.contains(method) || absoluteNavigationMethodSet.contains(method) || relativeNavigationMethodSet.contains(method) || method.equals(closeMethod) || method.equals(cancelRowUpdatesMethod))
		{
			return new DriverWriteInvocationStrategy<D, ResultSet, Object>();
		}
		
		if (transactionalWriteMethodSet.contains(method))
		{
			return this.transactionContext.start(new DatabaseWriteInvocationStrategy<D, ResultSet, Object>(this.cluster.getTransactionalExecutor()), this.getParent().getConnection());
		}
		
		if (method.equals(getStatementMethod))
		{
			return new InvocationStrategy<D, ResultSet, S>()
			{
				public S invoke(SQLProxy<D, ResultSet> proxy, Invoker<D, ResultSet, S> invoker) throws Exception
				{
					return ResultSetInvocationHandler.this.getParent();
				}
			};
		}
		
		if (getBlobMethodSet.contains(method))
		{
			return new BlobInvocationStrategy<D, ResultSet>(this.cluster, resultSet);
		}
		
		if (getClobMethodSet.contains(method))
		{
			return new ClobInvocationStrategy<D, ResultSet>(this.cluster, resultSet, method.getReturnType().asSubclass(Clob.class));
		}
		
		if (this.isGetMethod(method))
		{
			return new DriverReadInvocationStrategy<D, ResultSet, Object>();
		}
		
		if (this.isUpdateMethod(method))
		{
			return new DriverWriteInvocationStrategy<D, ResultSet, Object>();
		}
		
		return super.getInvocationStrategy(resultSet, method, parameters);
	}

	/**
	 * @see net.sf.hajdbc.sql.AbstractChildInvocationHandler#getInvoker(java.lang.Object, java.lang.reflect.Method, java.lang.Object[])
	 */
	@SuppressWarnings("unchecked")
	@Override
	protected Invoker<D, ResultSet, ?> getInvoker(ResultSet object, final Method method, final Object[] parameters) throws Exception
	{
		Class<?>[] types = method.getParameterTypes();
		
		if (this.isUpdateMethod(method) && (parameters.length > 1) && (parameters[1] != null))
		{
			Class<?> type = types[1];
			
			if (type.equals(InputStream.class))
			{
				final File file = this.fileSupport.createFile((InputStream) parameters[1]);
				
				return new Invoker<D, ResultSet, Object>()
				{
					public Object invoke(Database<D> database, ResultSet resultSet) throws SQLException
					{
						List<Object> parameterList = new ArrayList<Object>(Arrays.asList(parameters));
						
						parameterList.set(1, ResultSetInvocationHandler.this.fileSupport.getInputStream(file));
						
						return Methods.invoke(method, resultSet, parameterList.toArray());
					}				
				};
			}
			
			if (type.equals(Reader.class))
			{
				final File file = this.fileSupport.createFile((Reader) parameters[1]);
				
				return new Invoker<D, ResultSet, Object>()
				{
					public Object invoke(Database<D> database, ResultSet resultSet) throws SQLException
					{
						List<Object> parameterList = new ArrayList<Object>(Arrays.asList(parameters));
						
						parameterList.set(1, ResultSetInvocationHandler.this.fileSupport.getReader(file));
						
						return Methods.invoke(method, resultSet, parameterList.toArray());
					}				
				};
			}
			
			if (type.equals(Blob.class))
			{
				Blob blob = (Blob) parameters[1];
				
				if (Proxy.isProxyClass(blob.getClass()))
				{
					InvocationHandler handler = Proxy.getInvocationHandler(blob);
					
					if (SQLProxy.class.isInstance(handler))
					{
						final SQLProxy<D, Blob> proxy = (SQLProxy) handler;
						
						return new Invoker<D, ResultSet, Object>()
						{
							public Object invoke(Database<D> database, ResultSet resultSet) throws SQLException
							{
								List<Object> parameterList = new ArrayList<Object>(Arrays.asList(parameters));
								
								parameterList.set(1, proxy.getObject(database));
								
								return Methods.invoke(method, resultSet, parameterList.toArray());
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
					
					if (SQLProxy.class.isInstance(handler))
					{
						final SQLProxy<D, Clob> proxy = (SQLProxy) handler;
						
						return new Invoker<D, ResultSet, Object>()
						{
							public Object invoke(Database<D> database, ResultSet resultSet) throws SQLException
							{
								List<Object> parameterList = new ArrayList<Object>(Arrays.asList(parameters));
								
								parameterList.set(1, proxy.getObject(database));
								
								return Methods.invoke(method, resultSet, parameterList.toArray());
							}				
						};
					}
				}

				Clob serialClob = new SerialClob(clob);
				
				parameters[1] = type.equals(Clob.class) ? serialClob : ProxyFactory.createProxy(type, new SimpleInvocationHandler(serialClob));
			}
		}
		
		return super.getInvoker(object, method, parameters);
	}

	/**
	 * @see net.sf.hajdbc.sql.AbstractChildInvocationHandler#postInvoke(java.lang.Object, java.lang.reflect.Method, java.lang.Object[])
	 */
	@Override
	protected void postInvoke(ResultSet object, Method method, Object[] parameters)
	{
		if (method.equals(closeMethod))
		{
			this.getParentProxy().removeChild(this);
		}
	}

	/**
	 * @see net.sf.hajdbc.sql.AbstractChildInvocationHandler#handleFailures(java.util.SortedMap)
	 */
	@Override
	public <R> SortedMap<Database<D>, R> handlePartialFailure(SortedMap<Database<D>, R> resultMap, SortedMap<Database<D>, Exception> exceptionMap) throws Exception
	{
		return this.getParentProxy().handlePartialFailure(resultMap, exceptionMap);
	}

	/**
	 * @see net.sf.hajdbc.sql.AbstractChildInvocationHandler#close(java.lang.Object, java.lang.Object)
	 */
	@Override
	protected void close(S statement, ResultSet resultSet) throws SQLException
	{
		resultSet.close();
	}

	/**
	 * @see net.sf.hajdbc.sql.AbstractInvocationHandler#record(net.sf.hajdbc.sql.Invoker, java.lang.reflect.Method, java.lang.Object[])
	 */
	@Override
	protected void record(Invoker<D, ResultSet, ?> invoker, Method method, Object[] parameters)
	{
		if (this.isUpdateMethod(method))
		{
			synchronized (this.updateInvokerList)
			{
				this.updateInvokerList.add(invoker);
			}
		}
		else if (transactionalWriteMethodSet.contains(method) || method.equals(cancelRowUpdatesMethod))
		{
			synchronized (this.updateInvokerList)
			{
				this.updateInvokerList.clear();
			}
		}
		else if (absoluteNavigationMethodSet.contains(method))
		{
			synchronized (this.relativeNavigationInvokerList)
			{
				this.absoluteNavigationInvoker = invoker;
				this.relativeNavigationInvokerList.clear();
			}
		}
		else if (relativeNavigationMethodSet.contains(method))
		{
			synchronized (this.relativeNavigationInvokerList)
			{
				this.relativeNavigationInvokerList.add(invoker);
			}
		}
		else
		{
			super.record(invoker, method, parameters);
		}
	}

	/**
	 * @see net.sf.hajdbc.sql.AbstractInvocationHandler#isRecordable(java.lang.reflect.Method)
	 */
	@Override
	protected boolean isRecordable(Method method)
	{
		return driverWriteMethodSet.contains(method);
	}

	/**
	 * @see net.sf.hajdbc.sql.AbstractInvocationHandler#replay(net.sf.hajdbc.Database, java.lang.Object)
	 */
	@Override
	protected void replay(Database<D> database, ResultSet resultSet) throws Exception
	{
		super.replay(database, resultSet);
		
		synchronized (this.relativeNavigationInvokerList)
		{
			if (this.absoluteNavigationInvoker != null)
			{
				this.absoluteNavigationInvoker.invoke(database, resultSet);
			}
			
			for (Invoker<D, ResultSet, ?> invoker: this.relativeNavigationInvokerList)
			{
				invoker.invoke(database, resultSet);
			}
		}
		
		synchronized (this.updateInvokerList)
		{
			for (Invoker<D, ResultSet, ?> invoker: this.updateInvokerList)
			{
				invoker.invoke(database, resultSet);
			}
		}
	}
	
	private boolean isGetMethod(Method method)
	{
		Class<?>[] types = method.getParameterTypes();
		
		return method.getName().startsWith("get") && (types != null) && (types.length > 0) && (types[0].equals(String.class) || types[0].equals(Integer.TYPE));
	}
	
	private boolean isUpdateMethod(Method method)
	{
		Class<?>[] types = method.getParameterTypes();
		
		return method.getName().startsWith("update") && (types != null) && (types.length > 0) && (types[0].equals(String.class) || types[0].equals(Integer.TYPE));
	}
}
