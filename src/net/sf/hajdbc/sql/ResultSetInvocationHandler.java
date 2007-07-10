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
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.locks.Lock;

import javax.sql.rowset.serial.SerialBlob;
import javax.sql.rowset.serial.SerialClob;

import net.sf.hajdbc.Database;
import net.sf.hajdbc.util.SQLExceptionFactory;
import net.sf.hajdbc.util.SimpleInvocationHandler;
import net.sf.hajdbc.util.reflect.ProxyFactory;

/**
 * @author Paul Ferraro
 *
 */
public class ResultSetInvocationHandler<D, S extends Statement> extends AbstractInvocationHandler<D, S, ResultSet>
{
	private static final Set<String> DRIVER_READ_METHOD_SET = new HashSet<String>(Arrays.asList("findColumn", "getConcurrency", "getCursorName", "getFetchDirection", "getFetchSize", "getHoldability", "getMetaData", "getRow", "getType", "getWarnings", "isAfterLast", "isBeforeFirst", "isClosed", "isFirst", "isLast", "rowDeleted", "rowInserted", "rowUpdated", "wasNull"));
	private static final Set<String> DRIVER_WRITE_METHOD_SET = new HashSet<String>(Arrays.asList("absolute", "afterLast", "beforeFirst", "cancelRowUpdates", "clearWarnings", "first", "last", "moveToCurrentRow", "moveToInsertRow", "next", "previous", "relative", "setFetchDirection", "setFetchSize"));
	private static final Set<String> DATABASE_WRITE_METHOD_SET = new HashSet<String>(Arrays.asList("deleteRow", "insertRow", "updateRow"));
	
	protected FileSupport fileSupport;
	
	/**
	 * @param statement the statement that created this result set
	 * @param proxy the invocation handler of the statement that created this result set
	 * @param invoker the invoker that was used to create this result set
	 * @param resultSetMap a map of database to underlying result set
	 * @param fileSupport support for streams
	 * @throws Exception
	 */
	protected ResultSetInvocationHandler(S statement, SQLProxy<D, S> proxy, Invoker<D, S, ResultSet> invoker, Map<Database<D>, ResultSet> resultSetMap, FileSupport fileSupport) throws Exception
	{
		super(statement, proxy, invoker, ResultSet.class, resultSetMap);
		
		this.fileSupport = fileSupport;
	}

	/**
	 * @see net.sf.hajdbc.sql.AbstractInvocationHandler#getInvocationStrategy(java.lang.Object, java.lang.reflect.Method, java.lang.Object[])
	 */
	@Override
	protected InvocationStrategy<D, ResultSet, ?> getInvocationStrategy(ResultSet resultSet, Method method, Object[] parameters) throws Exception
	{
		Class<?>[] types = method.getParameterTypes();
		String methodName = method.getName();
		
		if (DRIVER_READ_METHOD_SET.contains(methodName))
		{
			return new DriverReadInvocationStrategy<D, ResultSet, Object>();
		}
		
		if (DRIVER_WRITE_METHOD_SET.contains(methodName))
		{
			return new DriverWriteInvocationStrategy<D, ResultSet, Object>();
		}
		
		if (DATABASE_WRITE_METHOD_SET.contains(methodName))
		{
			List<Lock> lockList = Collections.emptyList();
			
			return new DatabaseWriteInvocationStrategy<D, ResultSet, Object>(lockList);
		}
		
		if (methodName.startsWith("get") && (types != null) && (types.length > 0) && ((types[0].equals(Integer.TYPE) || types[0].equals(String.class))))
		{
			Class<?> returnClass = method.getReturnType();
			
			if (returnClass.equals(Blob.class))
			{
				return new BlobInvocationStrategy<D, ResultSet>(resultSet);
			}
			
			if (Clob.class.isAssignableFrom(returnClass))
			{
				return new ClobInvocationStrategy<D, ResultSet>(resultSet, returnClass.asSubclass(Clob.class));
			}
			
			return new DriverReadInvocationStrategy<D, ResultSet, Object>();
		}
		
		if (method.equals(ResultSet.class.getMethod("getStatement")))
		{
			return new InvocationStrategy<D, ResultSet, S>()
			{
				public S invoke(SQLProxy<D, ResultSet> proxy, Invoker<D, ResultSet, S> invoker) throws Exception
				{
					return ResultSetInvocationHandler.this.getParent();
				}
			};
		}
		
		if (methodName.startsWith("update") && (types != null) && (types.length > 0) && (types[0].equals(String.class) || types[0].equals(Integer.TYPE)))
		{
			return new DriverWriteInvocationStrategy<D, ResultSet, Object>();
		}
		
		return super.getInvocationStrategy(resultSet, method, parameters);
	}

	/**
	 * @see net.sf.hajdbc.sql.AbstractInvocationHandler#getInvoker(java.lang.Object, java.lang.reflect.Method, java.lang.Object[])
	 */
	@SuppressWarnings("unchecked")
	@Override
	protected Invoker<D, ResultSet, ?> getInvoker(ResultSet object, final Method method, final Object[] parameters) throws Exception
	{
		Class<?>[] types = method.getParameterTypes();
		String methodName = method.getName();
		
		if (methodName.startsWith("update") && (types != null) && (types.length > 1) && (types[0].equals(String.class) || types[0].equals(Integer.TYPE)))
		{
			if (types[1].equals(InputStream.class))
			{
				final File file = this.fileSupport.createFile((InputStream) parameters[1]);
				
				return new Invoker<D, ResultSet, Object>()
				{
					public Object invoke(Database<D> database, ResultSet resultSet) throws SQLException
					{
						List<Object> parameterList = new ArrayList<Object>(Arrays.asList(parameters));
						
						parameterList.set(1, ResultSetInvocationHandler.this.fileSupport.getInputStream(file));
						
						try
						{
							return method.invoke(resultSet, parameterList.toArray());
						}
						catch (IllegalAccessException e)
						{
							throw SQLExceptionFactory.createSQLException(e);
						}
						catch (InvocationTargetException e)
						{
							throw SQLExceptionFactory.createSQLException(e.getTargetException());
						}
					}				
				};
			}
			
			if (types[1].equals(Reader.class))
			{
				final File file = this.fileSupport.createFile((Reader) parameters[1]);
				
				return new Invoker<D, ResultSet, Object>()
				{
					public Object invoke(Database<D> database, ResultSet resultSet) throws SQLException
					{
						List<Object> parameterList = new ArrayList<Object>(Arrays.asList(parameters));
						
						parameterList.set(1, ResultSetInvocationHandler.this.fileSupport.getReader(file));
						
						try
						{
							return method.invoke(resultSet, parameterList.toArray());
						}
						catch (IllegalAccessException e)
						{
							throw SQLExceptionFactory.createSQLException(e);
						}
						catch (InvocationTargetException e)
						{
							throw SQLExceptionFactory.createSQLException(e.getTargetException());
						}
					}				
				};
			}
			
			if (types[1].equals(Blob.class))
			{
				if (Proxy.isProxyClass(parameters[1].getClass()))
				{
					final BlobInvocationHandler<D, ?> proxy = (BlobInvocationHandler) Proxy.getInvocationHandler(parameters[1]);
					
					return new Invoker<D, ResultSet, Object>()
					{
						public Object invoke(Database<D> database, ResultSet resultSet) throws SQLException
						{
							List<Object> parameterList = new ArrayList<Object>(Arrays.asList(parameters));
							
							parameterList.set(1, proxy.getObject(database));
							
							try
							{
								return method.invoke(resultSet, parameterList.toArray());
							}
							catch (IllegalAccessException e)
							{
								throw SQLExceptionFactory.createSQLException(e);
							}
							catch (InvocationTargetException e)
							{
								throw SQLExceptionFactory.createSQLException(e.getTargetException());
							}
						}				
					};
				}

				parameters[1] = new SerialBlob((Blob) parameters[1]);
			}
			
			// Handle both clob and nclob
			if (Clob.class.isAssignableFrom(types[1]))
			{
				if (Proxy.isProxyClass(parameters[1].getClass()))
				{
					final ClobInvocationHandler<D, ?> proxy = (ClobInvocationHandler) Proxy.getInvocationHandler(parameters[1]);
					
					return new Invoker<D, ResultSet, Object>()
					{
						public Object invoke(Database<D> database, ResultSet resultSet) throws SQLException
						{
							List<Object> parameterList = new ArrayList<Object>(Arrays.asList(parameters));
							
							parameterList.set(1, proxy.getObject(database));
							
							try
							{
								return method.invoke(resultSet, parameterList.toArray());
							}
							catch (IllegalAccessException e)
							{
								throw SQLExceptionFactory.createSQLException(e);
							}
							catch (InvocationTargetException e)
							{
								throw SQLExceptionFactory.createSQLException(e.getTargetException());
							}
						}				
					};
				}

				Clob clob = new SerialClob((Clob) parameters[1]);
				
				parameters[1] = types[1].equals(Clob.class) ? clob : ProxyFactory.createProxy(types[1], new SimpleInvocationHandler(clob));
			}
		}
		
		return super.getInvoker(object, method, parameters);
	}

	/**
	 * @see net.sf.hajdbc.sql.AbstractInvocationHandler#postInvoke(java.lang.Object, java.lang.reflect.Method, java.lang.Object[])
	 */
	@Override
	protected void postInvoke(ResultSet object, Method method, Object[] parameters) throws Exception
	{
		if (method.equals(ResultSet.class.getMethod("close")))
		{
			this.getParentProxy().removeChild(this);
		}
	}

	/**
	 * @see net.sf.hajdbc.sql.AbstractInvocationHandler#handleFailures(java.util.SortedMap)
	 */
	@Override
	public <R> SortedMap<Database<D>, R> handlePartialFailure(SortedMap<Database<D>, R> resultMap, SortedMap<Database<D>, SQLException> exceptionMap) throws SQLException
	{
		return this.getParentProxy().handlePartialFailure(resultMap, exceptionMap);
	}

	/**
	 * @see net.sf.hajdbc.sql.AbstractInvocationHandler#close(java.lang.Object, java.lang.Object)
	 */
	@Override
	protected void close(S statement, ResultSet resultSet) throws SQLException
	{
		resultSet.close();
	}
}
