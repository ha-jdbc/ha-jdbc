/*
 * HA-JDBC: High-Availability JDBC
 * Copyright (c) 2004-2007 Paul Ferraro
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

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.Set;

import net.sf.hajdbc.Database;

/**
 * @author Paul Ferraro
 * @param <D> 
 * @param <P> 
 * @param <E> 
 */
public abstract class LocatorInvocationHandler<D, P, E> extends AbstractChildInvocationHandler<D, P, E>
{
	/**
	 * @param parent
	 * @param proxy
	 * @param invoker
	 * @param proxyClass 
	 * @param objectMap
	 * @throws Exception
	 */
	protected LocatorInvocationHandler(P parent, SQLProxy<D, P> proxy, Invoker<D, P, E> invoker, Class<E> proxyClass, Map<Database<D>, E> objectMap) throws Exception
	{
		super(parent, proxy, invoker, proxyClass, objectMap);
	}

	/**
	 * @see net.sf.hajdbc.sql.AbstractChildInvocationHandler#getInvocationStrategy(java.lang.Object, java.lang.reflect.Method, java.lang.Object[])
	 */
	@Override
	protected InvocationStrategy<D, E, ?> getInvocationStrategy(E object, Method method, Object[] parameters) throws Exception
	{
		String methodName = method.getName();
		
		if (this.getDatabaseReadMethodSet().contains(methodName))
		{
			return new DatabaseReadInvocationStrategy<D, E, Object>();
		}
		
		return super.getInvocationStrategy(object, method, parameters);
	}

	protected abstract Set<String> getDatabaseReadMethodSet();
	
	/**
	 * @see net.sf.hajdbc.sql.AbstractChildInvocationHandler#postInvoke(java.lang.Object, java.lang.reflect.Method, java.lang.Object[])
	 */
	@SuppressWarnings("nls")
	@Override
	protected void postInvoke(E object, Method method, Object[] parameters)
	{
		if (method.getName().equals("free"))
		{
			this.getParentProxy().removeChild(this);
		}
	}

	/**
	 * @see net.sf.hajdbc.sql.AbstractChildInvocationHandler#close(java.lang.Object, java.lang.Object)
	 */
	@SuppressWarnings("nls")
	@Override
	protected void close(P parent, E locator)
	{
		try
		{
			// free() is a Java 1.6 method
			locator.getClass().getMethod("free").invoke(locator);
		}
		catch (NoSuchMethodException e)
		{
			// Ignore
		}
		catch (IllegalAccessException e)
		{
			// Ignore
		}
		catch (InvocationTargetException e)
		{
			Throwable target = e.getTargetException();
			this.logger.warn(target.getMessage(), target);
		}
	}
}
