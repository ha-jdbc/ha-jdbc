/*
 * HA-JDBC: High-Availability JDBC
 * Copyright (c) 2004-2008 Paul Ferraro
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
package net.sf.hajdbc.sql.pool;

import java.lang.reflect.Method;
import java.sql.SQLException;
import java.util.Map;
import java.util.Set;

import javax.sql.CommonDataSource;
import javax.sql.PooledConnection;

import net.sf.hajdbc.Database;
import net.sf.hajdbc.sql.AbstractChildInvocationHandler;
import net.sf.hajdbc.sql.ConnectionInvocationStrategy;
import net.sf.hajdbc.sql.DriverWriteInvocationStrategy;
import net.sf.hajdbc.sql.InvocationStrategy;
import net.sf.hajdbc.sql.Invoker;
import net.sf.hajdbc.sql.SQLProxy;
import net.sf.hajdbc.sql.TransactionContext;
import net.sf.hajdbc.util.reflect.Methods;

/**
 * @author Paul Ferraro
 * @param <D> 
 * @param <C> 
 */
@SuppressWarnings("nls")
public abstract class AbstractPooledConnectionInvocationHandler<D extends CommonDataSource, C extends PooledConnection> extends AbstractChildInvocationHandler<D, D, C>
{
	private static final Set<Method> eventListenerMethodSet = Methods.findMethods(PooledConnection.class, "(add|remove)(Connection|Statement)EventListener");
	
	private static final Method getConnectionMethod = Methods.getMethod(PooledConnection.class, "getConnection");
	private static final Method closeMethod = Methods.getMethod(PooledConnection.class, "close");
	
	/**
	 * @param dataSource
	 * @param proxy
	 * @param invoker
	 * @param proxyClass
	 * @param objectMap
	 * @throws Exception
	 */
	protected AbstractPooledConnectionInvocationHandler(D dataSource, SQLProxy<D, D> proxy, Invoker<D, D, C> invoker, Class<C> proxyClass, Map<Database<D>, C> objectMap) throws Exception
	{
		super(dataSource, proxy, invoker, proxyClass, objectMap);
	}

	@Override
	protected InvocationStrategy<D, C, ?> getInvocationStrategy(C connection, Method method, Object[] parameters) throws Exception
	{
		if (eventListenerMethodSet.contains(method))
		{
			return new DriverWriteInvocationStrategy<D, C, Void>();
		}
		
		if (method.equals(getConnectionMethod))
		{
			return new ConnectionInvocationStrategy<D, C>(this.cluster, connection, this.createTransactionContext());
		}

		return super.getInvocationStrategy(connection, method, parameters);
	}

	@Override
	protected void postInvoke(C connection, Method method, Object[] parameters)
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
	protected void close(D dataSource, C connection) throws SQLException
	{
		connection.close();
	}
	
	protected abstract TransactionContext<D> createTransactionContext();
}
