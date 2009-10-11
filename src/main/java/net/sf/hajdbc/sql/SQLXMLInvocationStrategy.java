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

public class SQLXMLInvocationStrategy<Z, D extends Database<Z>, P> extends LocatorInvocationStrategy<Z, D, P, java.sql.SQLXML>
{
	/**
	 * @param cluster 
	 * @param parent the object that created sql xml objects
	 * @throws SQLException 
	 */
	public SQLXMLInvocationStrategy(DatabaseCluster<Z, D> cluster, P parent, Connection connection) throws SQLException
	{
		super(cluster, parent, java.sql.SQLXML.class, connection);
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.sql.LocatorInvocationStrategy#createInvocationHandler(java.lang.Object, net.sf.hajdbc.sql.SQLProxy, net.sf.hajdbc.sql.Invoker, java.util.Map, boolean)
	 */
	@Override
	protected InvocationHandler createInvocationHandler(P parent, SQLProxy<Z, D, P, SQLException> proxy, Invoker<Z, D, P, java.sql.SQLXML, SQLException> invoker, Map<D, java.sql.SQLXML> objectMap, boolean updateCopy) throws SQLException
	{
		return new SQLXMLInvocationHandler<Z, D, P>(parent, proxy, invoker, objectMap, updateCopy);
	}
}
