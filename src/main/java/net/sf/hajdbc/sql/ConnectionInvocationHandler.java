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

import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.NClob;
import java.sql.SQLException;
import java.sql.Savepoint;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import net.sf.hajdbc.Database;
import net.sf.hajdbc.durability.Durability;
import net.sf.hajdbc.invocation.InvocationStrategies;
import net.sf.hajdbc.invocation.InvocationStrategy;
import net.sf.hajdbc.invocation.Invoker;
import net.sf.hajdbc.util.StaticRegistry;
import net.sf.hajdbc.util.reflect.Methods;

/**
 * @author Paul Ferraro
 * @param <D> 
 * @param <P> 
 */
public class ConnectionInvocationHandler<Z, D extends Database<Z>, P> extends ChildInvocationHandler<Z, D, P, SQLException, Connection, SQLException, ConnectionProxyFactory<Z, D, P>>
{
	private static final Set<Method> driverReadMethodSet = Methods.findMethods(Connection.class, "createStruct", "getAutoCommit", "getCatalog", "getClientInfo", "getHoldability", "getNetworkTimeout", "getSchema", "getTypeMap", "getWarnings", "isClosed", "isCloseOnCompletion", "isReadOnly", "nativeSQL");
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
	private static final Method createArrayMethod = Methods.getMethod(Connection.class, "createArrayOf", String.class, Object[].class);
	private static final Method createBlobMethod = Methods.getMethod(Connection.class, "createBlob");
	private static final Method createClobMethod = Methods.getMethod(Connection.class, "createClob");
	private static final Method createNClobMethod = Methods.getMethod(Connection.class, "createNClob");
	private static final Method createSQLXMLMethod = Methods.getMethod(Connection.class, "createSQLXML");
	
	private static final Set<Method> endTransactionMethodSet = new HashSet<Method>(Arrays.asList(commitMethod, rollbackMethod, setAutoCommitMethod));
	private static final Set<Method> createLocatorMethodSet = new HashSet<Method>(Arrays.asList(createBlobMethod, createClobMethod, createNClobMethod, createSQLXMLMethod));
	
	private static final StaticRegistry<Method, Durability.Phase> phaseRegistry = new DurabilityPhaseRegistry(Arrays.asList(commitMethod, setAutoCommitMethod), Arrays.asList(rollbackMethod));
	
	/**
	 * Constructs a new ConnectionInvocationHandler
	 * @param proxy
	 * @param handler
	 * @param invoker
	 * @param connectionMap
	 * @param transactionContext
	 */
	public ConnectionInvocationHandler(ConnectionProxyFactory<Z, D, P> map)
	{
		super(Connection.class, map, null);
	}
	
	@Override
	protected ProxyFactoryFactory<Z, D, Connection, SQLException, ?, ? extends Exception> getProxyFactoryFactory(Connection connection, Method method, Object... parameters) throws SQLException
	{
		if (createStatementMethodSet.contains(method))
		{
			return new StatementProxyFactoryFactory<Z, D>(this.getProxyFactory().getTransactionContext());
		}
		if (prepareStatementMethodSet.contains(method))
		{
			String sql = (String) parameters[0];
			return new PreparedStatementProxyFactoryFactory<Z, D>(this.getProxyFactory().getTransactionContext(), this.getProxyFactory().extractLocks(sql), this.getProxyFactory().isSelectForUpdate(sql));
		}
		if (prepareCallMethodSet.contains(method))
		{
			String sql = (String) parameters[0];
			return new CallableStatementProxyFactoryFactory<Z, D>(this.getProxyFactory().getTransactionContext(), this.getProxyFactory().extractLocks(sql));
		}
		
		if (setSavepointMethodSet.contains(method))
		{
			return new SavepointProxyFactoryFactory<Z, D>();
		}
		
		if (method.equals(getMetaDataMethod))
		{
			return new DatabaseMetaDataProxyFactoryFactory<Z, D>();
		}
		
		if (method.equals(createArrayMethod))
		{
			return new ArrayProxyFactoryFactory<Z, D, Connection>(this.getProxyFactory().locatorsUpdateCopy());
		}
		if (method.equals(createBlobMethod))
		{
			return new BlobProxyFactoryFactory<Z, D, Connection>(this.getProxyFactory().locatorsUpdateCopy());
		}
		if (method.equals(createClobMethod))
		{
			return new ClobProxyFactoryFactory<Z, D, Connection, Clob>(Clob.class, this.getProxyFactory().locatorsUpdateCopy());
		}
		if (method.equals(createNClobMethod))
		{
			return new ClobProxyFactoryFactory<Z, D, Connection, NClob>(NClob.class, this.getProxyFactory().locatorsUpdateCopy());
		}
		if (method.equals(createSQLXMLMethod))
		{
			return new SQLXMLProxyFactoryFactory<Z, D, Connection>(this.getProxyFactory().locatorsUpdateCopy());
		}
		
		return super.getProxyFactoryFactory(connection, method, parameters);
	}

	/**
	 * @throws SQLException 
	 * @see net.sf.hajdbc.sql.AbstractChildInvocationHandler#getInvocationStrategy(java.lang.Object, java.lang.reflect.Method, java.lang.Object[])
	 */
	@Override
	protected InvocationStrategy getInvocationStrategy(Connection connection, Method method, Object... parameters) throws SQLException
	{
		if (driverReadMethodSet.contains(method))
		{
			return InvocationStrategies.INVOKE_ON_ANY;
		}
		
		if (databaseReadMethodSet.contains(method) || method.equals(getMetaDataMethod))
		{
			return InvocationStrategies.INVOKE_ON_NEXT;
		}
		
		if (driverWriterMethodSet.contains(method) || method.equals(closeMethod) || createStatementMethodSet.contains(method))
		{
			return InvocationStrategies.INVOKE_ON_EXISTING;
		}
		
		if (prepareStatementMethodSet.contains(method) || prepareCallMethodSet.contains(method) || createLocatorMethodSet.contains(method))
		{
			return InvocationStrategies.INVOKE_ON_ALL;
		}
		
		if (endTransactionMethodSet.contains(method))
		{
			return this.getProxyFactory().getTransactionContext().end(InvocationStrategies.END_TRANSACTION_INVOKE_ON_ALL, phaseRegistry.get(method));
		}
		
		if (method.equals(rollbackSavepointMethod) || method.equals(releaseSavepointMethod))
		{
			return InvocationStrategies.END_TRANSACTION_INVOKE_ON_ALL;
		}
		
		if (setSavepointMethodSet.contains(method))
		{
			return InvocationStrategies.TRANSACTION_INVOKE_ON_ALL;
		}
		
		return super.getInvocationStrategy(connection, method, parameters);
	}

	/**
	 * @see net.sf.hajdbc.sql.AbstractChildInvocationHandler#getInvoker(java.lang.Object, java.lang.reflect.Method, java.lang.Object[])
	 */
	@Override
	protected <R> Invoker<Z, D, Connection, R, SQLException> getInvoker(Connection connection, Method method, Object... parameters) throws SQLException
	{
		if (method.equals(releaseSavepointMethod) || method.equals(rollbackSavepointMethod))
		{
			return this.getInvoker(Savepoint.class, 0, connection, method, parameters);
		}
		
		if (prepareStatementMethodSet.contains(method) || prepareCallMethodSet.contains(method))
		{
			parameters[0] = this.getProxyFactory().evaluate((String) parameters[0]);
		}

		Invoker<Z, D, Connection, R, SQLException> invoker = super.getInvoker(connection, method, parameters);
		
		if (endTransactionMethodSet.contains(method))
		{
			return this.getProxyFactory().getTransactionContext().end(invoker, phaseRegistry.get(method));
		}
		
		return invoker;
	}

	@Override
	protected <R> void postInvoke(Invoker<Z, D, Connection, R, SQLException> invoker, Connection proxy, Method method, Object... parameters)
	{
		if (driverWriterMethodSet.contains(method) || method.equals(setAutoCommitMethod))
		{
			this.getProxyFactory().record(invoker);
		}
		else if (method.equals(closeMethod))
		{
			this.getProxyFactory().getTransactionContext().close();
			this.getProxyFactory().remove();
		}
		else if (method.equals(releaseSavepointMethod))
		{
			SavepointInvocationHandler<Z, D> handler = (SavepointInvocationHandler<Z, D>) Proxy.getInvocationHandler(parameters[0]);
			this.getProxyFactory().removeChild(handler.getProxyFactory());
		}
	}
}
