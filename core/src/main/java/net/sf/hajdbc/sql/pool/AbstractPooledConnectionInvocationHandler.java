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
package net.sf.hajdbc.sql.pool;

import java.lang.reflect.Method;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import javax.sql.ConnectionEventListener;
import javax.sql.PooledConnection;
import javax.sql.StatementEventListener;

import net.sf.hajdbc.Database;
import net.sf.hajdbc.invocation.InvocationStrategies;
import net.sf.hajdbc.invocation.InvocationStrategy;
import net.sf.hajdbc.invocation.Invoker;
import net.sf.hajdbc.sql.ChildInvocationHandler;
import net.sf.hajdbc.sql.ConnectionProxyFactoryFactory;
import net.sf.hajdbc.sql.ProxyFactoryFactory;
import net.sf.hajdbc.util.reflect.Methods;

/**
 * @author Paul Ferraro
 * @param <D> 
 * @param <C> 
 */
@SuppressWarnings("nls")
public abstract class AbstractPooledConnectionInvocationHandler<Z, D extends Database<Z>, C extends PooledConnection, F extends AbstractPooledConnectionProxyFactory<Z, D, C>> extends ChildInvocationHandler<Z, D, Z, SQLException, C, SQLException, F>
{
	private static final Method addConnectionEventListenerMethod = Methods.getMethod(PooledConnection.class, "addConnectionEventListener", ConnectionEventListener.class);
	private static final Method addStatementEventListenerMethod = Methods.getMethod(PooledConnection.class, "addStatementEventListener", StatementEventListener.class);
	private static final Method removeConnectionEventListenerMethod = Methods.getMethod(PooledConnection.class, "removeConnectionEventListener", ConnectionEventListener.class);
	private static final Method removeStatementEventListenerMethod = Methods.getMethod(PooledConnection.class, "removeStatementEventListener", StatementEventListener.class);
	
	private static final Set<Method> eventListenerMethodSet = new HashSet<>(Arrays.asList(addConnectionEventListenerMethod, addStatementEventListenerMethod, removeConnectionEventListenerMethod, removeStatementEventListenerMethod));
	
	private static final Method getConnectionMethod = Methods.getMethod(PooledConnection.class, "getConnection");
	private static final Method closeMethod = Methods.getMethod(PooledConnection.class, "close");
	
	public AbstractPooledConnectionInvocationHandler(Class<C> proxyClass, F proxyFactory)
	{
		super(proxyClass, proxyFactory, null);
	}

	@Override
	protected ProxyFactoryFactory<Z, D, C, SQLException, ?, ? extends Exception> getProxyFactoryFactory(C object, Method method, Object... parameters) throws SQLException
	{
		if (method.equals(getConnectionMethod))
		{
			return new ConnectionProxyFactoryFactory<>(this.getProxyFactory().getTransactionContext());
		}
		
		return super.getProxyFactoryFactory(object, method, parameters);
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.sql.AbstractInvocationHandler#getInvocationStrategy(java.lang.Object, java.lang.reflect.Method, java.lang.Object[])
	 */
	@Override
	protected InvocationStrategy getInvocationStrategy(C connection, Method method, Object... parameters) throws SQLException
	{
		if (eventListenerMethodSet.contains(method))
		{
			return InvocationStrategies.INVOKE_ON_EXISTING;
		}

		return super.getInvocationStrategy(connection, method, parameters);
	}

	@Override
	protected <R> void postInvoke(Invoker<Z, D, C, R, SQLException> invoker, C proxy, Method method, Object... parameters)
	{
		super.postInvoke(invoker, proxy, method, parameters);
		
		if (method.equals(closeMethod))
		{
			this.getProxyFactory().remove();
		}
		else if (method.equals(addConnectionEventListenerMethod))
		{
			this.getProxyFactory().addConnectionEventListener((ConnectionEventListener) parameters[0], invoker);
		}
		else if (method.equals(removeConnectionEventListenerMethod))
		{
			this.getProxyFactory().removeConnectionEventListener((ConnectionEventListener) parameters[0]);
		}
		else if (method.equals(addStatementEventListenerMethod))
		{
			this.getProxyFactory().addStatementEventListener((StatementEventListener) parameters[0], invoker);
		}
		else if (method.equals(removeStatementEventListenerMethod))
		{
			this.getProxyFactory().removeStatementEventListener((StatementEventListener) parameters[0]);
		}
	}
}
