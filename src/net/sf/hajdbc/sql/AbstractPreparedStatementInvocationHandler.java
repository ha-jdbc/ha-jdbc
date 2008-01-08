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
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.sql.rowset.serial.SerialBlob;
import javax.sql.rowset.serial.SerialClob;

import net.sf.hajdbc.Database;
import net.sf.hajdbc.util.SimpleInvocationHandler;
import net.sf.hajdbc.util.reflect.Methods;
import net.sf.hajdbc.util.reflect.ProxyFactory;

/**
 * @author Paul Ferraro
 *
 */
@SuppressWarnings("nls")
public class AbstractPreparedStatementInvocationHandler<D, S extends PreparedStatement> extends AbstractStatementInvocationHandler<D, S>
{
	private static final Set<String> DATABASE_READ_METHOD_SET = new HashSet<String>(Arrays.asList("getMetaData", "getParameterMetaData"));
	private static final Set<String> DRIVER_WRITE_METHOD_SET = new HashSet<String>(Arrays.asList("addBatch", "clearParameters"));
	
	/**
	 * @param connection
	 * @param proxy
	 * @param invoker
	 * @param statementMap
	 * @throws Exception
	 */
	public AbstractPreparedStatementInvocationHandler(Connection connection, SQLProxy<D, Connection> proxy, Invoker<D, Connection, S> invoker, Class<S> statementClass, Map<Database<D>, S> statementMap, FileSupport fileSupport) throws Exception
	{
		super(connection, proxy, invoker, statementClass, statementMap, fileSupport);
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
			if (types[1].equals(InputStream.class))
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
			
			if (types[1].equals(Reader.class))
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
			
			if (types[1].equals(Blob.class))
			{
				if (Proxy.isProxyClass(parameters[1].getClass()))
				{
					final BlobInvocationHandler<D, ?> proxy = (BlobInvocationHandler) Proxy.getInvocationHandler(parameters[1]);
					
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

				parameters[1] = new SerialBlob((Blob) parameters[1]);
			}
		
			// Handle both clob and nclob
			if (Clob.class.isAssignableFrom(types[1]))
			{
				if (Proxy.isProxyClass(parameters[1].getClass()))
				{
					final ClobInvocationHandler<D, ?> proxy = (ClobInvocationHandler) Proxy.getInvocationHandler(parameters[1]);
					
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

				Clob clob = new SerialClob((Clob) parameters[1]);
				
				parameters[1] = types[1].equals(Clob.class) ? clob : ProxyFactory.createProxy(types[1], new SimpleInvocationHandler(clob));
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
		return super.isRecordable(method) || method.getName().equals("clearParameters") || this.isIndexSetMethod(method);
	}
}
