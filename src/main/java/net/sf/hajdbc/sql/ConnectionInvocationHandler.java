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
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Set;

import net.sf.hajdbc.Database;
import net.sf.hajdbc.ExceptionFactory;
import net.sf.hajdbc.durability.Durability;
import net.sf.hajdbc.util.reflect.Methods;

/**
 * @author Paul Ferraro
 * @param <D> 
 * @param <P> 
 */
@SuppressWarnings("nls")
public class ConnectionInvocationHandler<Z, D extends Database<Z>, P> extends AbstractChildInvocationHandler<Z, D, P, Connection, SQLException>
{
	private static final Set<Method> driverReadMethodSet = Methods.findMethods(Connection.class, "create(ArrayOf|Struct)", "getAutoCommit", "getCatalog", "getClientInfo", "getHoldability", "getTypeMap", "getWarnings", "isClosed", "isReadOnly", "nativeSQL");
	private static final Set<Method> databaseReadMethodSet = Methods.findMethods(Connection.class, "getTransactionIsolation", "isValid");
	private static final Set<Method> driverWriterMethodSet = Methods.findMethods(Connection.class, "clearWarnings", "setClientInfo", "setHoldability", "setTypeMap");
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
	
	private static final Map<Method, Durability.Phase> phaseMap = new IdentityHashMap<Method, Durability.Phase>();
	static
	{
		phaseMap.put(commitMethod, Durability.Phase.COMMIT);
		phaseMap.put(rollbackMethod, Durability.Phase.ROLLBACK);
		phaseMap.put(setAutoCommitMethod, Durability.Phase.COMMIT);
	}
	
	private TransactionContext<Z, D> transactionContext;
	
	/**
	 * @param proxy
	 * @param handler
	 * @param invoker
	 * @param connectionMap
	 * @param transactionContext 
	 * @throws Exception
	 */
	public ConnectionInvocationHandler(P proxy, SQLProxy<Z, D, P, SQLException> handler, Invoker<Z, D, P, Connection, SQLException> invoker, Map<D, Connection> connectionMap, TransactionContext<Z, D> transactionContext)
	{
		super(proxy, handler, invoker, Connection.class, connectionMap);
		
		this.transactionContext = transactionContext;
	}
	
	/**
	 * @see net.sf.hajdbc.sql.AbstractChildInvocationHandler#getInvocationStrategy(java.lang.Object, java.lang.reflect.Method, java.lang.Object[])
	 */
	@Override
	protected InvocationStrategy<Z, D, Connection, ?, SQLException> getInvocationStrategy(Connection connection, Method method, Object[] parameters) throws SQLException
	{
		if (driverReadMethodSet.contains(method))
		{
			return new DriverReadInvocationStrategy<Z, D, Connection, Object, SQLException>();
		}
		
		if (databaseReadMethodSet.contains(method))
		{
			return new DatabaseReadInvocationStrategy<Z, D, Connection, Object, SQLException>();
		}
		
		if (driverWriterMethodSet.contains(method) || method.equals(closeMethod))
		{
			return new DriverWriteInvocationStrategy<Z, D, Connection, Object, SQLException>();
		}
		
		if (endTransactionMethodSet.contains(method))
		{
			return this.transactionContext.end(new DatabaseWriteInvocationStrategy<Z, D, Connection, Void, SQLException>(this.cluster.getEndTransactionExecutor()), phaseMap.get(method));
		}
		
		if (method.equals(rollbackSavepointMethod) || method.equals(releaseSavepointMethod))
		{
			return new DatabaseWriteInvocationStrategy<Z, D, Connection, Void, SQLException>(this.cluster.getTransactionalExecutor());
		}
		
		if (createStatementMethodSet.contains(method))
		{
			return new StatementInvocationStrategy<Z, D>(connection, this.transactionContext);
		}
		
		if (prepareStatementMethodSet.contains(method))
		{
			return new PreparedStatementInvocationStrategy<Z, D>(this.cluster, connection, this.transactionContext, (String) parameters[0]);
		}
		
		if (prepareCallMethodSet.contains(method))
		{
			return new CallableStatementInvocationStrategy<Z, D>(this.cluster, connection, this.transactionContext);
		}
		
		if (setSavepointMethodSet.contains(method))
		{
			return new SavepointInvocationStrategy<Z, D>(this.cluster, connection);
		}
		
		if (method.equals(getMetaDataMethod))
		{
			return new DatabaseMetaDataInvocationStrategy<Z, D>(connection);
		}
		
		if (method.equals(createBlobMethod))
		{
			return new BlobInvocationStrategy<Z, D, Connection>(this.cluster, connection, connection);
		}

		if (method.equals(createClobMethod))
		{
			return new ClobInvocationStrategy<Z, D, Connection>(this.cluster, connection, connection);
		}

		if (method.equals(createNClobMethod))
		{
			return new NClobInvocationStrategy<Z, D, Connection>(this.cluster, connection, connection);
		}
		
		if (method.equals(createSQLXMLMethod))
		{
			return new SQLXMLInvocationStrategy<Z, D, Connection>(this.cluster, connection, connection);
		}
		
		return super.getInvocationStrategy(connection, method, parameters);
	}

	/**
	 * @see net.sf.hajdbc.sql.AbstractChildInvocationHandler#getInvoker(java.lang.Object, java.lang.reflect.Method, java.lang.Object[])
	 */
	@Override
	protected Invoker<Z, D, Connection, ?, SQLException> getInvoker(Connection connection, Method method, Object[] parameters) throws SQLException
	{
		if (method.equals(releaseSavepointMethod))
		{
			final SQLProxy<Z, D, Savepoint, SQLException> proxy = this.getInvocationHandler((Savepoint) parameters[0]);
			
			return new Invoker<Z, D, Connection, Void, SQLException>()
			{
				public Void invoke(D database, Connection connection) throws SQLException
				{
					connection.releaseSavepoint(proxy.getObject(database));
					
					return null;
				}					
			};
		}
		
		if (method.equals(rollbackSavepointMethod))
		{
			final SQLProxy<Z, D, Savepoint, SQLException> proxy = this.getInvocationHandler((Savepoint) parameters[0]);
			
			return new Invoker<Z, D, Connection, Void, SQLException>()
			{
				public Void invoke(D database, Connection connection) throws SQLException
				{
					connection.rollback(proxy.getObject(database));
					
					return null;
				}					
			};
		}
		
		Invoker<Z, D, Connection, ?, SQLException> invoker = super.getInvoker(connection, method, parameters);
		
		if (endTransactionMethodSet.contains(method))
		{
			return this.transactionContext.end(invoker, phaseMap.get(method));
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
	@SuppressWarnings("unchecked")
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
			SQLProxy<Z, D, Savepoint, SQLException> proxy = (SQLProxy) Proxy.getInvocationHandler(parameters[0]);
			
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
