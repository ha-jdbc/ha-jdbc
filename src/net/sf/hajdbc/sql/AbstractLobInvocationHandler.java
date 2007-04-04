/*
 * Copyright (c) 2004-2007, Identity Theft 911, LLC.  All rights reserved.
 */
package net.sf.hajdbc.sql;

import java.lang.reflect.Method;
import java.util.Map;
import java.util.Set;

import net.sf.hajdbc.Database;

/**
 * @author Paul Ferraro
 *
 */
public abstract class AbstractLobInvocationHandler<D, P, E> extends AbstractInvocationHandler<D, P, E>
{
	/**
	 * @param object
	 * @param proxy
	 * @param invoker
	 * @param objectMap
	 * @throws Exception
	 */
	protected AbstractLobInvocationHandler(P object, SQLProxy<D, P> proxy, Invoker<D, P, E> invoker, Map<Database<D>, E> objectMap) throws Exception
	{
		super(object, proxy, invoker, objectMap);
	}

	/**
	 * @see net.sf.hajdbc.sql.AbstractInvocationHandler#getInvocationStrategy(java.lang.Object, java.lang.reflect.Method, java.lang.Object[])
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
	 * @see net.sf.hajdbc.sql.AbstractInvocationHandler#postInvoke(java.lang.Object, java.lang.reflect.Method, java.lang.Object[])
	 */
	@Override
	protected void postInvoke(E object, Method method, Object[] parameters) throws Exception
	{
		if (method.getName().equals("free"))
		{
			this.getParentProxy().removeChild(this);
		}
	}

	/**
	 * @see net.sf.hajdbc.sql.AbstractInvocationHandler#close(java.lang.Object, java.lang.Object)
	 */
	@Override
	protected void close(P parent, E lob)
	{
		try
		{
			lob.getClass().getMethod("free").invoke(lob);
		}
		catch (Exception e)
		{
			// Ignore
		}
	}
}
