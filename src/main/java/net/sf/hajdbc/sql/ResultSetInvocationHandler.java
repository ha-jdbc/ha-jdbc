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
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.sql.rowset.serial.SerialBlob;
import javax.sql.rowset.serial.SerialClob;

import net.sf.hajdbc.Database;
import net.sf.hajdbc.ExceptionFactory;
import net.sf.hajdbc.util.reflect.Methods;
import net.sf.hajdbc.util.reflect.ProxyFactory;
import net.sf.hajdbc.util.reflect.SimpleInvocationHandler;

/**
 * @author Paul Ferraro
 * @param <D> 
 * @param <S> 
 */
@SuppressWarnings("nls")
public class ResultSetInvocationHandler<Z, D extends Database<Z>, S extends Statement> extends AbstractChildInvocationHandler<Z, D, S, ResultSet, SQLException>
{
	private static final Set<Method> driverReadMethodSet = Methods.findMethods(ResultSet.class, "findColumn", "getConcurrency", "getCursorName", "getFetchDirection", "getFetchSize", "getHoldability", "getMetaData", "getRow", "getType", "getWarnings", "isAfterLast", "isBeforeFirst", "isClosed", "isFirst", "isLast", "row(Deleted|Inserted|Updated)", "wasNull");
	private static final Set<Method> driverWriteMethodSet = Methods.findMethods(ResultSet.class, "absolute", "afterLast", "beforeFirst", "cancelRowUpdates", "clearWarnings", "first", "last", "moveTo(Current|Insert)Row", "next", "previous", "relative", "setFetchDirection", "setFetchSize");
	private static final Set<Method> transactionalWriteMethodSet = Methods.findMethods(ResultSet.class, "(delete|insert|update)Row");
	private static final Set<Method> getBlobMethodSet = Methods.findMethods(ResultSet.class, "getBlob");
	private static final Set<Method> getClobMethodSet = Methods.findMethods(ResultSet.class, "getClob");
	private static final Set<Method> getNClobMethodSet = Methods.findMethods(ResultSet.class, "getNClob");
	private static final Set<Method> getSQLXMLMethodSet = Methods.findMethods(ResultSet.class, "getSQLXML");
	
	private static final Method closeMethod = Methods.getMethod(ResultSet.class, "close");
	private static final Method getStatementMethod = Methods.getMethod(ResultSet.class, "getStatement");
	
	protected FileSupport<SQLException> fileSupport;
	private TransactionContext<Z, D> transactionContext;
	private List<Invoker<Z, D, ResultSet, ?, SQLException>> invokerList = new LinkedList<Invoker<Z, D, ResultSet, ?, SQLException>>();
	
	/**
	 * @param statement the statement that created this result set
	 * @param proxy the invocation handler of the statement that created this result set
	 * @param invoker the invoker that was used to create this result set
	 * @param resultSetMap a map of database to underlying result set
	 * @param transactionContext 
	 * @param fileSupport support for streams
	 * @throws Exception
	 */
	protected ResultSetInvocationHandler(S statement, SQLProxy<Z, D, S, SQLException> proxy, Invoker<Z, D, S, ResultSet, SQLException> invoker, Map<D, ResultSet> resultSetMap, TransactionContext<Z, D> transactionContext, FileSupport<SQLException> fileSupport)
	{
		super(statement, proxy, invoker, ResultSet.class, resultSetMap);
		
		this.transactionContext = transactionContext;
		this.fileSupport = fileSupport;
	}

	/**
	 * @see net.sf.hajdbc.sql.AbstractChildInvocationHandler#getInvocationStrategy(java.lang.Object, java.lang.reflect.Method, java.lang.Object[])
	 */
	@Override
	protected InvocationStrategy<Z, D, ResultSet, ?, SQLException> getInvocationStrategy(ResultSet resultSet, Method method, Object[] parameters) throws SQLException
	{
		if (driverReadMethodSet.contains(method))
		{
			return new DriverReadInvocationStrategy<Z, D, ResultSet, Object, SQLException>();
		}
		
		if (driverWriteMethodSet.contains(method) || method.equals(closeMethod))
		{
			return new DriverWriteInvocationStrategy<Z, D, ResultSet, Object, SQLException>();
		}
		
		if (transactionalWriteMethodSet.contains(method))
		{
			return this.transactionContext.start(new DatabaseWriteInvocationStrategy<Z, D, ResultSet, Object, SQLException>(this.cluster.getTransactionalExecutor()), this.getParent().getConnection());
		}
		
		if (method.equals(getStatementMethod))
		{
			return new InvocationStrategy<Z, D, ResultSet, S, SQLException>()
			{
				public S invoke(SQLProxy<Z, D, ResultSet, SQLException> proxy, Invoker<Z, D, ResultSet, S, SQLException> invoker)
				{
					return ResultSetInvocationHandler.this.getParent();
				}
			};
		}
		
		if (getBlobMethodSet.contains(method))
		{
			return new BlobInvocationStrategy<Z, D, ResultSet>(this.cluster, resultSet, this.getParent().getConnection());
		}
		
		if (getClobMethodSet.contains(method))
		{
			return new ClobInvocationStrategy<Z, D, ResultSet>(this.cluster, resultSet, this.getParent().getConnection());
		}
		
		if (getNClobMethodSet.contains(method))
		{
			return new NClobInvocationStrategy<Z, D, ResultSet>(this.cluster, resultSet, this.getParent().getConnection());
		}
		
		if (getSQLXMLMethodSet.contains(method))
		{
			return new SQLXMLInvocationStrategy<Z, D, ResultSet>(this.cluster, resultSet, this.getParent().getConnection());
		}
		
		if (this.isGetMethod(method))
		{
			return new DriverReadInvocationStrategy<Z, D, ResultSet, Object, SQLException>();
		}
		
		if (this.isUpdateMethod(method))
		{
			return new DriverWriteInvocationStrategy<Z, D, ResultSet, Object, SQLException>();
		}
		
		return super.getInvocationStrategy(resultSet, method, parameters);
	}

	/**
	 * @see net.sf.hajdbc.sql.AbstractChildInvocationHandler#getInvoker(java.lang.Object, java.lang.reflect.Method, java.lang.Object[])
	 */
	@Override
	protected Invoker<Z, D, ResultSet, ?, SQLException> getInvoker(ResultSet object, final Method method, final Object[] parameters) throws SQLException
	{
		if (this.isUpdateMethod(method) && (parameters.length > 1))
		{
			Object typeParameter = parameters[1];
			
			if (typeParameter != null)
			{
				Class<?> type = method.getParameterTypes()[1];
				
				if (type.equals(InputStream.class))
				{
					final File file = this.fileSupport.createFile((InputStream) typeParameter);
					
					return new Invoker<Z, D, ResultSet, Object, SQLException>()
					{
						public Object invoke(D database, ResultSet resultSet) throws SQLException
						{
							List<Object> parameterList = new ArrayList<Object>(Arrays.asList(parameters));
							
							parameterList.set(1, ResultSetInvocationHandler.this.fileSupport.getInputStream(file));
							
							return Methods.invoke(method, ResultSetInvocationHandler.this.getExceptionFactory(), resultSet, parameterList.toArray());
						}				
					};
				}
				
				if (type.equals(Reader.class))
				{
					final File file = this.fileSupport.createFile((Reader) typeParameter);
					
					return new Invoker<Z, D, ResultSet, Object, SQLException>()
					{
						public Object invoke(D database, ResultSet resultSet) throws SQLException
						{
							List<Object> parameterList = new ArrayList<Object>(Arrays.asList(parameters));
							
							parameterList.set(1, ResultSetInvocationHandler.this.fileSupport.getReader(file));
							
							return Methods.invoke(method, ResultSetInvocationHandler.this.getExceptionFactory(), resultSet, parameterList.toArray());
						}				
					};
				}
				
				if (type.equals(Blob.class))
				{
					Blob blob = (Blob) typeParameter;
					
					if (Proxy.isProxyClass(blob.getClass()) && (Proxy.getInvocationHandler(blob) instanceof SQLProxy<?, ?, ?, ?>))
					{
						final SQLProxy<Z, D, Blob, SQLException> proxy = this.getInvocationHandler(blob);
						
						return new Invoker<Z, D, ResultSet, Object, SQLException>()
						{
							public Object invoke(D database, ResultSet resultSet) throws SQLException
							{
								List<Object> parameterList = new ArrayList<Object>(Arrays.asList(parameters));
								
								parameterList.set(1, proxy.getObject(database));
								
								return Methods.invoke(method, ResultSetInvocationHandler.this.getExceptionFactory(), resultSet, parameterList.toArray());
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
						
						return new Invoker<Z, D, ResultSet, Object, SQLException>()
						{
							public Object invoke(D database, ResultSet resultSet) throws SQLException
							{
								List<Object> parameterList = new ArrayList<Object>(Arrays.asList(parameters));
								
								parameterList.set(1, proxy.getObject(database));
								
								return Methods.invoke(method, ResultSetInvocationHandler.this.getExceptionFactory(), resultSet, parameterList.toArray());
							}				
						};
					}

					Clob serialClob = new SerialClob(clob);
					
					parameters[1] = type.equals(Clob.class) ? serialClob : ProxyFactory.createProxy(type, new SimpleInvocationHandler(serialClob));
				}
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
	 * @see net.sf.hajdbc.sql.AbstractChildInvocationHandler#close(java.lang.Object, java.lang.Object)
	 */
	@Override
	protected void close(S statement, ResultSet resultSet) throws SQLException
	{
		resultSet.close();
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.sql.AbstractInvocationHandler#record(net.sf.hajdbc.sql.Invoker, java.lang.reflect.Method, java.lang.Object[])
	 */
	@Override
	protected void record(Invoker<Z, D, ResultSet, ?, SQLException> invoker, Method method, Object[] parameters)
	{
		if (driverWriteMethodSet.contains(method.getName()) || this.isUpdateMethod(method))
		{
			synchronized (this.invokerList)
			{
				this.invokerList.add(invoker);
			}
		}
		else
		{
			super.record(invoker, method, parameters);
		}
	}

	/**
	 * @see net.sf.hajdbc.sql.AbstractInvocationHandler#replay(net.sf.hajdbc.Database, java.lang.Object)
	 */
	@Override
	protected void replay(D database, ResultSet resultSet) throws SQLException
	{
		super.replay(database, resultSet);
		
		synchronized (this.invokerList)
		{
			for (Invoker<Z, D, ResultSet, ?, SQLException> invoker: this.invokerList)
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

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.sql.SQLProxy#getExceptionFactory()
	 */
	@Override
	public ExceptionFactory<SQLException> getExceptionFactory()
	{
		return SQLExceptionFactory.getInstance();
	}
}
