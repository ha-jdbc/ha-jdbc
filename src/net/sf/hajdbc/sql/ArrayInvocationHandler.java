/*
 * HA-JDBC: High-Availability JDBC
 * Copyright (c) 2004-2009 Paul Ferraro
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
import java.sql.Array;
import java.util.Map;
import java.util.Set;

import net.sf.hajdbc.Database;
import net.sf.hajdbc.util.reflect.Methods;

/**
 * @author paul
 *
 */
public class ArrayInvocationHandler<D, P> extends LocatorInvocationHandler<D, P, Array>
{
	private static final Set<Method> driverReadMethodSet = Methods.findMethods(Array.class, "getBaseType", "getBaseTypeName");
	private static final Set<Method> databaseReadMethodSet = Methods.findMethods(Array.class, "getArray", "getResultSet");
	
	/**
	 * @param parent
	 * @param proxy
	 * @param invoker
	 * @param proxyClass
	 * @param objectMap
	 * @param updateCopy
	 * @param readMethodSet
	 * @param writeMethodSet
	 * @throws Exception
	 */
	public ArrayInvocationHandler(P parent, SQLProxy<D, P> proxy, Invoker<D, P, Array> invoker, Map<Database<D>, Array> objectMap) throws Exception
	{
		super(parent, proxy, invoker, Array.class, objectMap);
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.sql.LocatorInvocationHandler#getInvocationStrategy(java.lang.Object, java.lang.reflect.Method, java.lang.Object[])
	 */
	@Override
	protected InvocationStrategy<D, Array, ?> getInvocationStrategy(Array object, Method method, Object[] parameters) throws Exception
	{
		if (driverReadMethodSet.contains(method))
		{
			return new DriverReadInvocationStrategy<D, Array, Object>();
		}
		
		return super.getInvocationStrategy(object, method, parameters);
	}

	/**
	 * @see net.sf.hajdbc.sql.LocatorInvocationHandler#getDatabaseReadMethodSet()
	 */
	@Override
	protected Set<Method> getDatabaseReadMethodSet()
	{
		return databaseReadMethodSet;
	}
}
