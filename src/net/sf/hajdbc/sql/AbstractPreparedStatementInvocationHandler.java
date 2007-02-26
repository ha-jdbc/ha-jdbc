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

import java.io.File;
import java.io.InputStream;
import java.io.Reader;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
public class AbstractPreparedStatementInvocationHandler<D, S extends PreparedStatement> extends AbstractStatementInvocationHandler<D, S>
{
	private static final Set<String> DATABASE_READ_METHOD_SET = new HashSet<String>(Arrays.asList("getMetaData", "getParameterMetaData"));
	private static final Set<String> DRIVER_WRITE_METHOD_SET = new HashSet<String>(Arrays.asList("addBatch", "clearParameters"));
	
	protected List<Lock> lockList;
	protected boolean selectForUpdate;
	
	/**
	 * @param connection
	 * @param proxy
	 * @param invoker
	 * @param statementMap
	 * @throws Exception
	 */
	public AbstractPreparedStatementInvocationHandler(Connection connection, SQLProxy<D, Connection> proxy, Invoker<D, Connection, S> invoker, Map<Database<D>, S> statementMap, FileSupport fileSupport, String sql) throws Exception
	{
		super(connection, proxy, invoker, statementMap, fileSupport);
		
		this.lockList = this.getLockList(sql);
		this.selectForUpdate = this.isSelectForUpdate(sql);
	}

	/**
	 * @see net.sf.hajdbc.sql.AbstractStatementInvocationHandler#getInvocationStrategy(java.sql.Statement, java.lang.reflect.Method, java.lang.Object[])
	 */
	@Override
	protected InvocationStrategy<D, S, ?> getInvocationStrategy(S statement, Method method, Object[] parameters) throws Exception
	{
		String methodName = method.getName();
		
		if (DATABASE_READ_METHOD_SET.contains(methodName))
		{
			return new DatabaseReadInvocationStrategy<D, S, Object>();
		}
		
		if (DRIVER_WRITE_METHOD_SET.contains(methodName))
		{
			return new DriverWriteInvocationStrategy<D, S, Object>();
		}
		
		if (methodName.startsWith("set"))
		{
			return new DriverWriteInvocationStrategy<D, S, Object>();
		}
		
		if (method.equals(PreparedStatement.class.getMethod("execute")) || method.equals(PreparedStatement.class.getMethod("executeUpdate")))
		{
			return new DatabaseWriteInvocationStrategy<D, S, Object>(this.lockList);
		}
		
		if (method.equals(PreparedStatement.class.getMethod("executeQuery")))
		{
			if ((this.lockList.isEmpty() && !this.selectForUpdate && (statement.getResultSetConcurrency() == java.sql.ResultSet.CONCUR_READ_ONLY)))
			{
				return new DatabaseReadInvocationStrategy<D, S, Object>();
			}
			
			return new EagerResultSetInvocationStrategy<D, S>(statement, this.fileSupport, this.lockList);
		}
		
		return super.getInvocationStrategy(statement, method, parameters);
	}

	/**
	 * @see net.sf.hajdbc.sql.AbstractInvocationHandler#getInvoker(java.lang.Object, java.lang.reflect.Method, java.lang.Object[])
	 */
	@SuppressWarnings("unchecked")
	@Override
	protected Invoker<D, S, ?> getInvoker(S statement, final Method method, final Object[] parameters) throws Exception
	{
		String methodName = method.getName();
		Class<?>[] types = method.getParameterTypes();
		
		if (methodName.startsWith("set") && (types != null) && (types.length > 1) && (types[0].equals(String.class) || types[0].equals(Integer.TYPE)))
		{
			if (types[1].equals(InputStream.class))
			{
				final File file = this.fileSupport.createFile(InputStream.class.cast(parameters[1]));
				
				return new Invoker<D, S, Object>()
				{
					public Object invoke(Database<D> database, S statement) throws SQLException
					{
						List<Object> parameterList = new ArrayList<Object>(Arrays.asList(parameters));
						
						parameterList.set(1, AbstractPreparedStatementInvocationHandler.this.fileSupport.getInputStream(file));
						
						try
						{
							return method.invoke(statement, parameterList.toArray());
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
				final File file = this.fileSupport.createFile(Reader.class.cast(parameters[1]));
				
				return new Invoker<D, S, Object>()
				{
					public Object invoke(Database<D> database, S statement) throws SQLException
					{
						List<Object> parameterList = new ArrayList<Object>(Arrays.asList(parameters));
						
						parameterList.set(1, AbstractPreparedStatementInvocationHandler.this.fileSupport.getReader(file));
						
						try
						{
							return method.invoke(statement, parameterList.toArray());
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
					final BlobInvocationHandler<D, ?> proxy = BlobInvocationHandler.class.cast(Proxy.getInvocationHandler(parameters[1]));
					
					return new Invoker<D, S, Object>()
					{
						public Object invoke(Database<D> database, S statement) throws SQLException
						{
							List<Object> parameterList = new ArrayList<Object>(Arrays.asList(parameters));
							
							parameterList.set(1, proxy.getObject(database));
							
							try
							{
								return method.invoke(statement, parameterList.toArray());
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

				parameters[1] = new SerialBlob(Blob.class.cast(parameters[1]));
			}
		
			// Handle both clob and nclob
			if (Clob.class.isAssignableFrom(types[1]))
			{
				if (Proxy.isProxyClass(parameters[1].getClass()))
				{
					final ClobInvocationHandler<D, ?> proxy = ClobInvocationHandler.class.cast(Proxy.getInvocationHandler(parameters[1]));
					
					return new Invoker<D, S, Object>()
					{
						public Object invoke(Database<D> database, S statement) throws SQLException
						{
							List<Object> parameterList = new ArrayList<Object>(Arrays.asList(parameters));
							
							parameterList.set(1, proxy.getObject(database));
							
							try
							{
								return method.invoke(statement, parameterList.toArray());
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

				Clob clob = new SerialClob(Clob.class.cast(parameters[1]));
				
				parameters[1] = types[1].equals(Clob.class) ? clob : ProxyFactory.createProxy(types[1], new SimpleInvocationHandler(clob));
			}
		}
		
		return super.getInvoker(statement, method, parameters);
	}
}
