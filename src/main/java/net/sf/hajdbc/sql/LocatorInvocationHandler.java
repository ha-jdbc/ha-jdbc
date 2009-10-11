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

import java.lang.reflect.Method;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import net.sf.hajdbc.Database;
import net.sf.hajdbc.ExceptionFactory;
import net.sf.hajdbc.util.reflect.Methods;

/**
 * @author Paul Ferraro
 * @param <D> 
 * @param <P> 
 * @param <E> 
 */
public abstract class LocatorInvocationHandler<Z, D extends Database<Z>, P, E> extends AbstractChildInvocationHandler<Z, D, P, E, SQLException>
{
	private final Method freeMethod;
	private final Set<Method> readMethodSet;
	private final Set<Method> writeMethodSet;
	private final List<Invoker<Z, D, E, ?, SQLException>> invokerList = new LinkedList<Invoker<Z, D, E, ?, SQLException>>();
	private final boolean updateCopy;
	
	/**
	 * @param parent
	 * @param proxy
	 * @param invoker
	 * @param proxyClass 
	 * @param objectMap
	 * @throws Exception
	 */
	protected LocatorInvocationHandler(P parent, SQLProxy<Z, D, P, SQLException> proxy, Invoker<Z, D, P, E, SQLException> invoker, Class<E> proxyClass, Map<D, E> objectMap, boolean updateCopy, Set<Method> readMethodSet, Set<Method> writeMethodSet)
	{
		super(parent, proxy, invoker, proxyClass, objectMap);
		
		this.freeMethod = Methods.findMethod(proxyClass, "free");
		this.updateCopy = updateCopy;
		this.readMethodSet = readMethodSet;
		this.writeMethodSet = writeMethodSet;
	}

	/**
	 * @see net.sf.hajdbc.sql.AbstractChildInvocationHandler#getInvocationStrategy(java.lang.Object, java.lang.reflect.Method, java.lang.Object[])
	 */
	@Override
	protected InvocationStrategy<Z, D, E, ?, SQLException> getInvocationStrategy(E object, Method method, Object[] parameters) throws SQLException
	{
		if (this.readMethodSet.contains(method))
		{
			return this.updateCopy ? new DriverReadInvocationStrategy<Z, D, E, Object, SQLException>() : new DatabaseReadInvocationStrategy<Z, D, E, Object, SQLException>();
		}
		
		if (this.updateCopy && this.writeMethodSet.contains(method))
		{
			return new DriverWriteInvocationStrategy<Z, D, E, Object, SQLException>();
		}
		
		return super.getInvocationStrategy(object, method, parameters);
	}
	
	/**
	 * @see net.sf.hajdbc.sql.AbstractChildInvocationHandler#postInvoke(java.lang.Object, java.lang.reflect.Method, java.lang.Object[])
	 */
	@SuppressWarnings("nls")
	@Override
	protected void postInvoke(E object, Method method, Object[] parameters)
	{
		if ((this.freeMethod != null) && method.equals(this.freeMethod))
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
		if (this.freeMethod != null)
		{
			try
			{
				this.free(locator);
			}
			catch (SQLException e)
			{
				this.logger.warn(e.getMessage(), e);
			}
		}
	}
	
	protected abstract void free(E locator) throws SQLException;

	/**
	 * @see net.sf.hajdbc.sql.AbstractInvocationHandler#record(net.sf.hajdbc.sql.Invoker, java.lang.reflect.Method, java.lang.Object[])
	 */
	@Override
	protected void record(Invoker<Z, D, E, ?, SQLException> invoker, Method method, Object[] parameters)
	{
		if (this.isRecordable(method))
		{
			synchronized (this.invokerList)
			{
				this.invokerList.add(invoker);
			}
		}
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.sql.SQLProxy#getExceptionFactory()
	 */
	@Override
	public ExceptionFactory<SQLException> getExceptionFactory()
	{
		return SQLExceptionFactory.getInstance();
	}
}
