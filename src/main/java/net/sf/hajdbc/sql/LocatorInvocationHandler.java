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

import java.lang.reflect.Method;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import net.sf.hajdbc.Database;
import net.sf.hajdbc.logging.Level;
import net.sf.hajdbc.util.reflect.Methods;

/**
 * @author Paul Ferraro
 * @param <Z>
 * @param <D>
 * @param <P>
 * @param <T>
 */
public abstract class LocatorInvocationHandler<Z, D extends Database<Z>, P, T> extends ChildInvocationHandler<Z, D, P, T, SQLException>
{
	private final Method freeMethod;
	private final Set<Method> readMethodSet;
	private final Set<Method> writeMethodSet;
	private final List<Invoker<Z, D, T, ?, SQLException>> invokerList = new LinkedList<Invoker<Z, D, T, ?, SQLException>>();
	private final boolean updateCopy;
	
	/**
	 * Constructs a new LocatorInvocationHandler
	 * @param parent
	 * @param proxy
	 * @param invoker
	 * @param locatorClass
	 * @param locators
	 * @param updateCopy
	 * @param readMethodSet
	 * @param writeMethodSet
	 */
	protected LocatorInvocationHandler(P parent, SQLProxy<Z, D, P, SQLException> proxy, Invoker<Z, D, P, T, SQLException> invoker, Class<T> locatorClass, Map<D, T> locators, boolean updateCopy, Set<Method> readMethodSet, Set<Method> writeMethodSet)
	{
		super(parent, proxy, invoker, locatorClass, SQLException.class, locators);
		
		this.freeMethod = Methods.findMethod(locatorClass, "free");
		this.updateCopy = updateCopy;
		this.readMethodSet = readMethodSet;
		this.writeMethodSet = writeMethodSet;
	}

	/**
	 * @see net.sf.hajdbc.sql.AbstractChildInvocationHandler#getInvocationStrategy(java.lang.Object, java.lang.reflect.Method, java.lang.Object[])
	 */
	@Override
	protected InvocationStrategy getInvocationStrategy(T locator, Method method, Object[] parameters) throws SQLException
	{
		if (this.readMethodSet.contains(method))
		{
			return this.updateCopy ? InvocationStrategyEnum.INVOKE_ON_ANY : InvocationStrategyEnum.INVOKE_ON_NEXT;
		}
		
		if (this.updateCopy && this.writeMethodSet.contains(method))
		{
			return InvocationStrategyEnum.INVOKE_ON_EXISTING;
		}
		
		return super.getInvocationStrategy(locator, method, parameters);
	}
	
	/**
	 * @see net.sf.hajdbc.sql.AbstractChildInvocationHandler#postInvoke(java.lang.Object, java.lang.reflect.Method, java.lang.Object[])
	 */
	@Override
	protected void postInvoke(T object, Method method, Object[] parameters)
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
	protected void close(P parent, T locator)
	{
		if (this.freeMethod != null)
		{
			try
			{
				this.free(locator);
			}
			catch (SQLException e)
			{
				this.logger.log(Level.WARN, e, e.getMessage());
			}
		}
	}
	
	protected abstract void free(T locator) throws SQLException;

	/**
	 * @see net.sf.hajdbc.sql.AbstractInvocationHandler#record(net.sf.hajdbc.sql.Invoker, java.lang.reflect.Method, java.lang.Object[])
	 */
	@Override
	protected void record(Invoker<Z, D, T, ?, SQLException> invoker, Method method, Object[] parameters)
	{
		if (this.isRecordable(method))
		{
			synchronized (this.invokerList)
			{
				this.invokerList.add(invoker);
			}
		}
	}
}
