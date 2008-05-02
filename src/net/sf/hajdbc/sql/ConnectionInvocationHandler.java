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

import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Savepoint;
import java.util.Map;
import java.util.Set;

import net.sf.hajdbc.Database;
import net.sf.hajdbc.util.reflect.Methods;

/**
 * @author Paul Ferraro
 * @param <D> 
 * @param <P> 
 */
@SuppressWarnings("nls")
public class ConnectionInvocationHandler<D, P> extends AbstractChildInvocationHandler<D, P, Connection>
{
	private static final Set<Method> driverReadMethodSet = Methods.findMethods(Connection.class, "create(ArrayOf|Blob|Clob|NClob|SQLXML|Struct)", "getAutoCommit", "getCatalog", "getClientInfo", "getHoldability", "getTypeMap", "getWarnings", "isClosed", "isReadOnly", "nativeSQL");
	private static final Set<Method> databaseReadMethodSet = Methods.findMethods(Connection.class, "getMetaData", "getTransactionIsolation", "isValid");
	private static final Set<Method> driverWriterMethodSet = Methods.findMethods(Connection.class, "clearWarnings", "setAutoCommit", "setClientInfo", "setHoldability", "setTypeMap");
	private static final Set<Method> endTransactionMethodSet = Methods.findMethods(Connection.class, "commit", "rollback");
	private static final Set<Method> createStatementMethodSet = Methods.findMethods(Connection.class, "createStatement");
	private static final Set<Method> prepareStatementMethodSet = Methods.findMethods(Connection.class, "prepareStatement");
	private static final Set<Method> prepareCallMethodSet = Methods.findMethods(Connection.class, "prepareCall");
	private static final Set<Method> setSavepointMethodSet = Methods.findMethods(Connection.class, "setSavepoint");
	
	private static final Method releaseSavepointMethod = Methods.getMethod(Connection.class, "releaseSavepoint", Savepoint.class);
	private static final Method rollbackSavepointMethod = Methods.getMethod(Connection.class, "rollback", Savepoint.class);
	private static final Method closeMethod = Methods.getMethod(Connection.class, "close");
	
	private FileSupport fileSupport;
	private TransactionContext<D> transactionContext;
	
	/**
	 * @param proxy
	 * @param handler
	 * @param invoker
	 * @param connectionMap
	 * @param transactionContext 
	 * @param fileSupport 
	 * @throws Exception
	 */
	public ConnectionInvocationHandler(P proxy, SQLProxy<D, P> handler, Invoker<D, P, Connection> invoker, Map<Database<D>, Connection> connectionMap, TransactionContext<D> transactionContext, FileSupport fileSupport) throws Exception
	{
		super(proxy, handler, invoker, Connection.class, connectionMap);
		
		this.transactionContext = transactionContext;
		this.fileSupport = fileSupport;
	}
	
	/**
	 * @see net.sf.hajdbc.sql.AbstractChildInvocationHandler#getInvocationStrategy(java.lang.Object, java.lang.reflect.Method, java.lang.Object[])
	 */
	@Override
	protected InvocationStrategy<D, Connection, ?> getInvocationStrategy(Connection connection, Method method, Object[] parameters) throws Exception
	{
		if (driverReadMethodSet.contains(method))
		{
			return new DriverReadInvocationStrategy<D, Connection, Object>();
		}
		
		if (databaseReadMethodSet.contains(method))
		{
			return new DatabaseReadInvocationStrategy<D, Connection, Object>();
		}
		
		if (driverWriterMethodSet.contains(method) || method.equals(closeMethod))
		{
			return new DriverWriteInvocationStrategy<D, Connection, Object>();
		}
		
		if (endTransactionMethodSet.contains(method))
		{
			return this.transactionContext.end(new DatabaseWriteInvocationStrategy<D, Connection, Void>(this.cluster.getTransactionalExecutor()));
		}
		
		if (method.equals(rollbackSavepointMethod) || method.equals(releaseSavepointMethod))
		{
			return new DatabaseWriteInvocationStrategy<D, Connection, Void>(this.cluster.getTransactionalExecutor());
		}
		
		boolean createStatement = createStatementMethodSet.contains(method);
		boolean prepareStatement = prepareStatementMethodSet.contains(method);
		boolean prepareCall = prepareCallMethodSet.contains(method);
		
		if (createStatement || prepareStatement || prepareCall)
		{
			if (connection.isReadOnly())
			{
				return createStatement ? new DriverReadInvocationStrategy<D, Connection, Object>() : new DatabaseReadInvocationStrategy<D, Connection, Object>();
			}
			
			if (createStatement)
			{
				return new StatementInvocationStrategy<D>(connection, this.transactionContext, this.fileSupport);
			}
			
			if (prepareStatement)
			{
				return new PreparedStatementInvocationStrategy<D>(this.cluster, connection, this.transactionContext, this.fileSupport, (String) parameters[0]);
			}
			
			if (prepareCall)
			{
				return new CallableStatementInvocationStrategy<D>(this.cluster, connection, this.transactionContext, this.fileSupport);
			}
		}
		
		if (setSavepointMethodSet.contains(method))
		{
			return new SavepointInvocationStrategy<D>(this.cluster, connection);
		}
/*		
		if (methodName.equals("createBlob"))
		{
			return new BlobInvocationStrategy<D, Connection>(connection);
		}
		
		if (methodName.equals("createClob") || methodName.equals("createNClob"))
		{
			return new ClobInvocationStrategy<D, Connection>(connection, method.getReturnType().asSubclass(Clob.class));
		}
*/		
		return super.getInvocationStrategy(connection, method, parameters);
	}

	/**
	 * @see net.sf.hajdbc.sql.AbstractChildInvocationHandler#getInvoker(java.lang.Object, java.lang.reflect.Method, java.lang.Object[])
	 */
	@SuppressWarnings("unchecked")
	@Override
	protected Invoker<D, Connection, ?> getInvoker(Connection connection, Method method, Object[] parameters) throws Exception
	{
		if (method.equals(releaseSavepointMethod))
		{
			final SQLProxy<D, Savepoint> proxy = (SQLProxy) Proxy.getInvocationHandler(parameters[0]);
			
			return new Invoker<D, Connection, Void>()
			{
				public Void invoke(Database<D> database, Connection connection) throws SQLException
				{
					connection.releaseSavepoint(proxy.getObject(database));
					
					return null;
				}					
			};
		}
		
		if (method.equals(rollbackSavepointMethod))
		{
			final SQLProxy<D, Savepoint> proxy = (SQLProxy) Proxy.getInvocationHandler(parameters[0]);
			
			return new Invoker<D, Connection, Void>()
			{
				public Void invoke(Database<D> database, Connection connection) throws SQLException
				{
					connection.rollback(proxy.getObject(database));
					
					return null;
				}					
			};
		}
		
		return super.getInvoker(connection, method, parameters);
	}

	/**
	 * @see net.sf.hajdbc.sql.AbstractChildInvocationHandler#isSQLMethod(java.lang.reflect.Method)
	 */
	@Override
	protected boolean isSQLMethod(Method method)
	{
		return prepareStatementMethodSet.contains(method);
	}

	/**
	 * @see net.sf.hajdbc.sql.AbstractChildInvocationHandler#postInvoke(java.lang.Object, java.lang.reflect.Method, java.lang.Object[])
	 */
	@SuppressWarnings("unchecked")
	@Override
	protected void postInvoke(Connection object, Method method, Object[] parameters)
	{
		if (method.equals(closeMethod))
		{
			this.transactionContext.close();
			
			this.fileSupport.close();
			
			this.getParentProxy().removeChild(this);
		}
		else if (method.equals(releaseSavepointMethod))
		{
			SQLProxy<D, Savepoint> proxy = (SQLProxy) Proxy.getInvocationHandler(parameters[0]);
			
			this.removeChild(proxy);
		}
	}

	/**
	 * @see net.sf.hajdbc.sql.AbstractChildInvocationHandler#close(java.lang.Object, java.lang.Object)
	 */
	@Override
	protected void close(P parent, Connection connection) throws SQLException
	{
		connection.close();
	}
}
