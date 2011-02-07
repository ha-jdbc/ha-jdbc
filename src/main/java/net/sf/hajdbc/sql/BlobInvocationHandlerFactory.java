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
import java.sql.Blob;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;

import net.sf.hajdbc.Database;
import net.sf.hajdbc.invocation.Invoker;

/**
 * @author paul
 *
 */
public class BlobInvocationHandlerFactory<Z, D extends Database<Z>, P> extends LocatorInvocationHandlerFactory<Z, D, P, Blob>
{
	public BlobInvocationHandlerFactory(Connection connection)
	{
		super(Blob.class, connection);
	}
	
	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.sql.LocatorInvocationHandlerFactory#createInvocationHandler(java.lang.Object, net.sf.hajdbc.sql.SQLProxy, net.sf.hajdbc.invocation.Invoker, java.util.Map, boolean)
	 */
	@Override
	protected InvocationHandler createInvocationHandler(P parent, SQLProxy<Z, D, P, SQLException> proxy, Invoker<Z, D, P, Blob, SQLException> invoker, Map<D, Blob> locators, boolean updateCopy) throws SQLException
	{
		return new BlobInvocationHandler<Z, D, P>(parent, proxy, invoker, locators, updateCopy);
	}
}
