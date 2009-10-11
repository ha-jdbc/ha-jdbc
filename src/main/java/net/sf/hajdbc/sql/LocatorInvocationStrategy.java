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

import java.lang.reflect.InvocationHandler;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;

import net.sf.hajdbc.Database;
import net.sf.hajdbc.DatabaseCluster;
import net.sf.hajdbc.util.reflect.ProxyFactory;

/**
 * @author paul
 *
 */
public abstract class LocatorInvocationStrategy<Z, D extends Database<Z>, P, L> extends DatabaseWriteInvocationStrategy<Z, D, P, L, SQLException>
{
	private Class<? extends L> locatorClass;
	private P parent;
	private boolean updateCopy;
	
	/**
	 * @param executor
	 */
	public LocatorInvocationStrategy(DatabaseCluster<Z, D> cluster, P parent, Class<? extends L> locatorClass, Connection connection) throws SQLException
	{
		super(cluster.getNonTransactionalExecutor());
		
		this.locatorClass = locatorClass;
		this.parent = parent;
		this.updateCopy = cluster.getDatabaseMetaDataCache().getDatabaseProperties(cluster.getBalancer().next(), connection).locatorsUpdateCopy();
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.sql.DatabaseWriteInvocationStrategy#invoke(net.sf.hajdbc.sql.SQLProxy, net.sf.hajdbc.sql.Invoker)
	 */
	@Override
	public L invoke(SQLProxy<Z, D, P, SQLException> proxy, Invoker<Z, D, P, L, SQLException> invoker) throws SQLException
	{
		return ProxyFactory.createProxy(this.locatorClass, this.createInvocationHandler(this.parent, proxy, invoker, this.invokeAll(proxy, invoker), this.updateCopy));
	}

	protected abstract InvocationHandler createInvocationHandler(P parent, SQLProxy<Z, D, P, SQLException> proxy, Invoker<Z, D, P, L, SQLException> invoker, Map<D, L> objectMap, boolean updateCopy) throws SQLException;
}
