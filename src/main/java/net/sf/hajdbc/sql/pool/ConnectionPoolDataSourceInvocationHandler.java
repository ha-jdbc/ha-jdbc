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
import java.util.Set;

import javax.sql.ConnectionPoolDataSource;

import net.sf.hajdbc.DatabaseCluster;
import net.sf.hajdbc.invocation.InvocationStrategy;
import net.sf.hajdbc.invocation.InvocationStrategyEnum;
import net.sf.hajdbc.sql.CommonDataSourceInvocationHandler;
import net.sf.hajdbc.sql.InvocationHandlerFactory;
import net.sf.hajdbc.util.reflect.Methods;

/**
 * @author Paul Ferraro
 *
 */
@SuppressWarnings("nls")
public class ConnectionPoolDataSourceInvocationHandler extends CommonDataSourceInvocationHandler<ConnectionPoolDataSource, ConnectionPoolDataSourceDatabase>
{
	private static final Set<Method> getPooledConnectionMethodSet = Methods.findMethods(ConnectionPoolDataSource.class, "getPooledConnection");
	
	/**
	 * @param databaseCluster
	 */
	public ConnectionPoolDataSourceInvocationHandler(DatabaseCluster<ConnectionPoolDataSource, ConnectionPoolDataSourceDatabase> databaseCluster)
	{
		super(databaseCluster, ConnectionPoolDataSource.class);
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.sql.CommonDataSourceInvocationHandler#getInvocationStrategy(javax.sql.CommonDataSource, java.lang.reflect.Method, java.lang.Object[])
	 */
	@Override
	protected InvocationStrategy getInvocationStrategy(ConnectionPoolDataSource dataSource, Method method, Object[] parameters) throws SQLException
	{
		if (getPooledConnectionMethodSet.contains(method))
		{
			return InvocationStrategyEnum.TRANSACTION_INVOKE_ON_ALL;
		}
		return super.getInvocationStrategy(dataSource, method, parameters);
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.sql.AbstractInvocationHandler#getInvocationHandlerFactory(java.lang.Object, java.lang.reflect.Method, java.lang.Object[])
	 */
	@Override
	protected InvocationHandlerFactory<ConnectionPoolDataSource, ConnectionPoolDataSourceDatabase, ConnectionPoolDataSource, ?, SQLException> getInvocationHandlerFactory(ConnectionPoolDataSource object, Method method, Object[] parameters) throws SQLException
	{
		if (getPooledConnectionMethodSet.contains(method))
		{
			return new PooledConnectionInvocationHandlerFactory();
		}
		
		return super.getInvocationHandlerFactory(object, method, parameters);
	}
}
