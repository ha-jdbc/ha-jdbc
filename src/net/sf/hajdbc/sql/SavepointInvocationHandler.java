/*
 * HA-JDBC: High-Availability JDBC
 * Copyright (c) 2004-2006 Paul Ferraro
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
package net.sf.hajdbc.sql;

import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.Savepoint;
import java.util.Map;

import net.sf.hajdbc.Database;

/**
 * @author Paul Ferraro
 *
 */
public class SavepointInvocationHandler<D> extends AbstractInvocationHandler<D, Connection, Savepoint>
{
	/**
	 * @param object
	 * @param proxy
	 * @param invoker
	 * @param objectMap
	 * @throws Exception
	 */
	protected SavepointInvocationHandler(Connection connection, SQLProxy<D, Connection> proxy, Invoker<D, Connection, Savepoint> invoker, Map<Database<D>, Savepoint> savepointMap) throws Exception
	{
		super(connection, proxy, invoker, savepointMap);
	}

	/**
	 * @see net.sf.hajdbc.sql.AbstractInvocationHandler#getInvocationStrategy(java.lang.Object, java.lang.reflect.Method, java.lang.Object[])
	 */
	@Override
	protected InvocationStrategy<D, Savepoint, ?> getInvocationStrategy(Savepoint object, Method method, Object[] parameters)
	{
		return new DriverReadInvocationStrategy<D, Savepoint, Object>();
	}
}
