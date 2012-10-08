/*
 * HA-JDBC: High-Availability JDBC
 * Copyright 2004-2009 Paul Ferraro
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
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Savepoint;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import net.sf.hajdbc.Database;
import net.sf.hajdbc.durability.Durability;
import net.sf.hajdbc.invocation.InvocationStrategy;
import net.sf.hajdbc.invocation.InvocationStrategyEnum;
import net.sf.hajdbc.invocation.Invoker;
import net.sf.hajdbc.util.StaticRegistry;
import net.sf.hajdbc.util.reflect.Methods;

/**
 * @author Paul Ferraro
 * @param <D> 
 * @param <P> 
 */
@SuppressWarnings("nls")
public class ConnectionInvocationHandler<Z, D extends Database<Z>, P> extends ChildInvocationHandler<Z, D, P, Connection, SQLException>
{
	private static final Set<Method> driverReadMethodSet = Methods.findMethods(Connection.class, "create(ArrayOf|Struct)", "getAutoCommit", "getCatalog", "getClientInfo", "getHoldability", "getNetworkTimeout", "getSchema", "getTypeMap", "getWarnings", "isClosed", "isCloseOnCompletion", "isReadOnly", "nativeSQL");
	private static final Set<Method> databaseReadMethodSet = Methods.findMethods(Connection.class, "getTransactionIsolation", "isValid");
	private static final Set<Method> driverWriterMethodSet = Methods.findMethods(Connection.class, "abort", "clearWarnings", "closeOnCompletion", "setClientInfo", "setHoldability", "setNetworkTimeout", "setSchema", "setTypeMap");
	private static final Set<Method> createStatementMethodSet = Methods.findMethods(Connection.class, "createStatement");
	private static final Set<Method> prepareStatementMethodSet = Methods.findMethods(Connection.class, "prepareStatement");
	private static final Set<Method> prepareCallMethodSet = Methods.findMethods(Connection.class, "prepareCall");
	private static final Set<Method> setSavepointMethodSet = Methods.findMethods(Connection.class, "setSavepoint");

	private static final Method setAutoCommitMethod = Methods.getMethod(Connection.class, "setAutoCommit", Boolean.TYPE);
	private static final Method commitMethod = Methods.getMethod(Connection.class, "commit");
	private static final Method rollbackMethod = Methods.getMethod(Connection.class, "rollback");
	private static final Method getMetaDataMethod = Methods.getMethod(Connection.class, "getMetaData");
	private static final Method releaseSavepointMethod = Methods.getMethod(Connection.class, "releaseSavepoint", Savepoint.class);
	private static final Method rollbackSavepointMethod = Methods.getMethod(Connection.class, "rollback", Savepoint.class);
	private static final Method closeMethod = Methods.getMethod(Connection.class, "close");
	private static final Method createBlobMethod = Methods.getMethod(Connection.class, "createBlob");
	private static final Method createClobMethod = Methods.getMethod(Connection.class, "createClob");
	private static final Method createNClobMethod = Methods.getMethod(Connection.class, "createNClob");
	private static final Method createSQLXMLMethod = Methods.getMethod(Connection.class, "createSQLXML");
	
	private static final Set<Method> endTransactionMethodSet = new HashSet<Method>(Arrays.asList(commitMethod, rollbackMethod, setAutoCommitMethod));
	private static final Set<Method> createLocatorMethodSet = new HashSet<Method>(Arrays.asList(createBlobMethod, createClobMethod, createNClobMethod, createSQLXMLMethod));
	
	private static final StaticRegistry<Method, Durability.Phase> phaseRegistry = new DurabilityPhaseRegistry(Arrays.asList(commitMethod, setAutoCommitMethod), Arrays.asList(rollbackMethod));
	
	private TransactionContext<Z, D> transactionContext;
	
	/**
	 * Constructs a new ConnectionInvocationHandler
	 * @param proxy
	 * @param handler
	 * @param invoker
	 * @param connectionMap
	 * @param transactionContext
	 */
	public ConnectionInvocationHandler(P proxy, SQLProxy<Z, D, P, SQLException> handler, Invoker<Z, D, P, Connection, SQLException> invoker, Map<D, Connection> connectionMap, TransactionContext<Z, D> transactionContext)
	{
		super(proxy, handler, invoker, Connection.class, SQLException.class, connectionMap);
		
		this.transactionContext = transactionContext;
	}
	
	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.sql.AbstractInvocationHandler#getInvocationHandlerFactory(java.lang.Object, java.lang.reflect.Method, java.lang.Object[])
	 */
	@Override
	protected InvocationHandlerFactory<Z, D, Connection, ?, SQLException> getInvocationHandlerFactory(Connection connection, Method method, Object[] parameters)
	{
		if (createStatementMethodSet.contains(method))
		{
			return new StatementInvocationHandlerFactory<Z, D>(this.transactionContext);
		}
		
		if (prepareStatementMethodSet.contains(method))
		{
			return new PreparedStatementInvocationHandlerFactory<Z, D>(this.transactionContext, (String) parameters[0]);
		}
		
		if (prepareCallMethodSet.contains(method))
		{
			return new CallableStatementInvocationHandlerFactory<Z, D>(this.transactionContext);
		}
		
		if (setSavepointMethodSet.contains(method))
		{
			return new SavepointInvocationHandlerFactory<Z, D>();
		}
		
		if (method.equals(getMetaDataMethod))
		{
			return new DatabaseMetaDataInvocationHandlerFactory<Z, D>();
		}
		
		if (method.equals(createBlobMethod))
		{
			return new BlobInvocationHandlerFactory<Z, D, Connection>(connection);
		}

		if (method.equals(createClobMethod))
		{
			return new ClobInvocationHandlerFactory<Z, D, Connection>(connection);
		}

		if (method.equals(createNClobMethod))
		{
			return new NClobInvocationHandlerFactory<Z, D, Connection>(connection);
		}
		
		if (method.equals(createSQLXMLMethod))
		{
			return new SQLXMLInvocationHandlerFactory<Z, D, Connection>(connection);
		}
		
		return null;
	}

	/**
	 * @throws SQLException 
	 * @see net.sf.hajdbc.sql.AbstractChildInvocationHandler#getInvocationStrategy(java.lang.Object, java.lang.reflect.Method, java.lang.Object[])
	 */
	@Override
	protected InvocationStrategy getInvocationStrategy(Connection connection, Method method, Object[] parameters) throws SQLException
	{
		if (driverReadMethodSet.contains(method))
		{
			return InvocationStrategyEnum.INVOKE_ON_ANY;
		}
		
		if (databaseReadMethodSet.contains(method) || method.equals(getMetaDataMethod))
		{
			return InvocationStrategyEnum.INVOKE_ON_NEXT;
		}
		
		if (driverWriterMethodSet.contains(method) || method.equals(closeMethod) || createStatementMethodSet.contains(method))
		{
			return InvocationStrategyEnum.INVOKE_ON_EXISTING;
		}
		
		if (prepareStatementMethodSet.contains(method) || prepareCallMethodSet.contains(method) || createLocatorMethodSet.contains(method))
		{
			return InvocationStrategyEnum.INVOKE_ON_ALL;
		}
		
		if (endTransactionMethodSet.contains(method))
		{
			return this.transactionContext.end(InvocationStrategyEnum.END_TRANSACTION_INVOKE_ON_ALL, phaseRegistry.get(method));
		}
		
		if (method.equals(rollbackSavepointMethod) || method.equals(releaseSavepointMethod))
		{
			return InvocationStrategyEnum.END_TRANSACTION_INVOKE_ON_ALL;
		}
		
		if (setSavepointMethodSet.contains(method))
		{
			return InvocationStrategyEnum.TRANSACTION_INVOKE_ON_ALL;
		}
		
		return super.getInvocationStrategy(connection, method, parameters);
	}

	/**
	 * @see net.sf.hajdbc.sql.AbstractChildInvocationHandler#getInvoker(java.lang.Object, java.lang.reflect.Method, java.lang.Object[])
	 */
	@Override
	protected <R> Invoker<Z, D, Connection, R, SQLException> getInvoker(Connection connection, Method method, Object[] parameters) throws SQLException
	{
		if (method.equals(releaseSavepointMethod))
		{
			final SQLProxy<Z, D, Savepoint, SQLException> proxy = this.getInvocationHandler((Savepoint) parameters[0]);
			
			return new Invoker<Z, D, Connection, R, SQLException>()
			{
				@Override
				public R invoke(D database, Connection connection) throws SQLException
				{
					connection.releaseSavepoint(proxy.getObject(database));
					
					return null;
				}					
			};
		}
		
		if (method.equals(rollbackSavepointMethod))
		{
			final SQLProxy<Z, D, Savepoint, SQLException> proxy = this.getInvocationHandler((Savepoint) parameters[0]);
			
			return new Invoker<Z, D, Connection, R, SQLException>()
			{
				@Override
				public R invoke(D database, Connection connection) throws SQLException
				{
					connection.rollback(proxy.getObject(database));
					
					return null;
				}					
			};
		}
		
		Invoker<Z, D, Connection, R, SQLException> invoker = super.getInvoker(connection, method, parameters);
		
		if (endTransactionMethodSet.contains(method))
		{
			return this.transactionContext.end(invoker, phaseRegistry.get(method));
		}
		
		return invoker;
	}

	/**
	 * @see net.sf.hajdbc.sql.AbstractChildInvocationHandler#isSQLMethod(java.lang.reflect.Method)
	 */
	@Override
	protected boolean isSQLMethod(Method method)
	{
		return prepareStatementMethodSet.contains(method);
	}

	@Override
	protected boolean isRecordable(Method method)
	{
		return driverWriterMethodSet.contains(method) || method.equals(setAutoCommitMethod);
	}

	/**
	 * @see net.sf.hajdbc.sql.AbstractChildInvocationHandler#postInvoke(java.lang.Object, java.lang.reflect.Method, java.lang.Object[])
	 */
	@Override
	protected void postInvoke(Connection object, Method method, Object[] parameters)
	{
		if (method.equals(closeMethod))
		{
			this.transactionContext.close();
			
			this.getParentProxy().removeChild(this);
		}
		else if (method.equals(releaseSavepointMethod))
		{
			@SuppressWarnings("unchecked")
			SQLProxy<Z, D, Savepoint, SQLException> proxy = (SQLProxy<Z, D, Savepoint, SQLException>) Proxy.getInvocationHandler(parameters[0]);
			
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
