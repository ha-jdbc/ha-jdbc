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

import java.util.TreeMap;

import net.sf.hajdbc.Database;
import net.sf.hajdbc.DatabaseCluster;

/**
 * @author Paul Ferraro
 * @param <D> 
 */
public abstract class RootInvocationHandler<Z, D extends Database<Z>, E extends Exception> extends AbstractInvocationHandler<Z, D, Z, E> implements RootSQLProxy<Z, D, E>
{
	private final DatabaseCluster<Z, D> cluster;
	
	/**
	 * Constructs a new AbstractRootInvocationHandler.
	 * @param databaseCluster
	 * @param proxyClass
	 */
	protected RootInvocationHandler(DatabaseCluster<Z, D> cluster, Class<Z> proxyClass, Class<E> exceptionClass)
	{
		super(proxyClass, exceptionClass, new TreeMap<D, Z>());
		
		this.cluster = cluster;
		
		for (D database: cluster.getBalancer())
		{
			this.getObject(database);
		}
	}
	
	/**
	 * @see net.sf.hajdbc.sql.SQLProxy#getDatabaseCluster()
	 */
	@Override
	public DatabaseCluster<Z, D> getDatabaseCluster()
	{
		return this.cluster;
	}

	/**
	 * @see net.sf.hajdbc.sql.AbstractInvocationHandler#close(net.sf.hajdbc.Database, java.lang.Object)
	 */
	@Override
	protected void close(D database, Z object)
	{
		// Nothing to close
	}

	/**
	 * @see net.sf.hajdbc.sql.AbstractInvocationHandler#createObject(net.sf.hajdbc.Database)
	 */
	@Override
	protected Z createObject(D database) throws E
	{
		return database.createConnectionSource();
	}

	/**
	 * @see net.sf.hajdbc.sql.SQLProxy#getRoot()
	 */
	@Override
	public RootSQLProxy<Z, D, ? extends Exception> getRoot()
	{
		return this;
	}
}
