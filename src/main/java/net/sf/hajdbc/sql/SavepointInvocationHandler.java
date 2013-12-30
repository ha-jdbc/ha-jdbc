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
package net.sf.hajdbc.sql;

import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Savepoint;

import net.sf.hajdbc.Database;
import net.sf.hajdbc.invocation.InvocationStrategies;
import net.sf.hajdbc.invocation.InvocationStrategy;

/**
 * @author Paul Ferraro
 * @param <D> 
 */
public class SavepointInvocationHandler<Z, D extends Database<Z>> extends ChildInvocationHandler<Z, D, Connection, SQLException, Savepoint, SQLException, SavepointProxyFactory<Z, D>>
{
	/**
	 * @param factory a factory for creating savepoint proxies.
	 */
	public SavepointInvocationHandler(SavepointProxyFactory<Z, D> factory)
	{
		super(Savepoint.class, factory, null);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected InvocationStrategy getInvocationStrategy(Savepoint savepoint, Method method, Object... parameters)
	{
		return InvocationStrategies.INVOKE_ON_ANY;
	}
}
