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
import java.sql.Array;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Set;

import net.sf.hajdbc.Database;
import net.sf.hajdbc.invocation.InvocationStrategies;
import net.sf.hajdbc.invocation.InvocationStrategy;
import net.sf.hajdbc.util.reflect.Methods;

/**
 * @author paul
 *
 */
public class ArrayInvocationHandler<Z, D extends Database<Z>, P> extends LocatorInvocationHandler<Z, D, P, Array, ArrayProxyFactory<Z, D, P>>
{
	private static final Set<Method> DRIVER_READ_METHODS = Methods.findMethods(Array.class, "getBaseType", "getBaseTypeName");
	private static final Set<Method> READ_METHODS = Methods.findMethods(Array.class, "getArray", "getResultSet");
	private static final Set<Method> WRITE_METHODS = Collections.emptySet();
	
	protected ArrayInvocationHandler(ArrayProxyFactory<Z, D, P> proxyFactory)
	{
		super(Array.class, proxyFactory, READ_METHODS, WRITE_METHODS);
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.sql.LocatorInvocationHandler#getInvocationStrategy(java.lang.Object, java.lang.reflect.Method, java.lang.Object[])
	 */
	@Override
	protected InvocationStrategy getInvocationStrategy(Array array, Method method, Object... parameters) throws SQLException
	{
		if (DRIVER_READ_METHODS.contains(method))
		{
			return InvocationStrategies.INVOKE_ON_ANY;
		}
		
		return super.getInvocationStrategy(array, method, parameters);
	}
}
