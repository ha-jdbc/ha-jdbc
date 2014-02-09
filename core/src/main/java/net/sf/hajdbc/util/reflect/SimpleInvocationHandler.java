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
package net.sf.hajdbc.util.reflect;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

/**
 * A trivial invocation handler implementation.
 * @author Paul Ferraro
 */
public class SimpleInvocationHandler implements InvocationHandler
{
	private Object object;
	
	/**
	 * Constructs a new invocation handler that proxies the specified object.
	 * @param object
	 */
	public SimpleInvocationHandler(Object object)
	{
		this.object = object;
	}
	
	/**
	 * @see java.lang.reflect.InvocationHandler#invoke(java.lang.Object, java.lang.reflect.Method, java.lang.Object[])
	 */
	@Override
	public Object invoke(Object proxy, Method method, Object[] parameters) throws Throwable
	{
		try
		{
			return method.invoke(this.object, parameters);
		}
		catch (InvocationTargetException e)
		{
			throw e.getTargetException();
		}
	}
}
