/*
 * HA-JDBC: High-Availablity JDBC
 * Copyright 2004-Apr 26, 2010 Paul Ferraro
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

import java.lang.reflect.InvocationHandler;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;

import net.sf.hajdbc.Database;
import net.sf.hajdbc.DatabaseCluster;
import net.sf.hajdbc.invocation.Invoker;

/**
 * @author paul
 *
 */
public abstract class LocatorInvocationHandlerFactory<Z, D extends Database<Z>, P, T> implements InvocationHandlerFactory<Z, D, P, T, SQLException>
{
	private final Class<T> locatorClass;
	private final Connection connection;
	
	protected LocatorInvocationHandlerFactory(Class<T> locatorClass, Connection connection)
	{
		this.locatorClass = locatorClass;
		this.connection = connection;
	}
	
	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.sql.InvocationHandlerFactory#createInvocationHandler(java.lang.Object, net.sf.hajdbc.sql.SQLProxy, net.sf.hajdbc.invocation.Invoker, java.util.Map)
	 */
	@Override
	public InvocationHandler createInvocationHandler(P parent, SQLProxy<Z, D, P, SQLException> proxy, Invoker<Z, D, P, T, SQLException> invoker, Map<D, T> locators) throws SQLException
	{
		DatabaseCluster<Z, D> cluster = proxy.getDatabaseCluster();
		
		return this.createInvocationHandler(parent, proxy, invoker, locators, cluster.getDatabaseMetaDataCache().getDatabaseProperties(cluster.getBalancer().next(), this.connection).locatorsUpdateCopy());
	}

	protected abstract InvocationHandler createInvocationHandler(P parent, SQLProxy<Z, D, P, SQLException> proxy, Invoker<Z, D, P, T, SQLException> invoker, Map<D, T> locators, boolean updateCopy) throws SQLException;
	
	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.sql.InvocationHandlerFactory#getTargetClass()
	 */
	@Override
	public Class<T> getTargetClass()
	{
		return this.locatorClass;
	}
}
