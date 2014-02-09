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
import java.sql.SQLException;
import java.util.Set;

import net.sf.hajdbc.Database;
import net.sf.hajdbc.invocation.InvocationStrategies;
import net.sf.hajdbc.invocation.InvocationStrategy;
import net.sf.hajdbc.invocation.Invoker;
import net.sf.hajdbc.util.reflect.Methods;

/**
 * @author Paul Ferraro
 * @param <Z>
 * @param <D>
 * @param <P>
 * @param <T>
 */
public abstract class LocatorInvocationHandler<Z, D extends Database<Z>, P, T, F extends LocatorProxyFactory<Z, D, P, T>> extends ChildInvocationHandler<Z, D, P, SQLException, T, SQLException, F>
{
	private final Method freeMethod;
	private final Set<Method> readMethods;
	private final Set<Method> writeMethods;
	
	protected LocatorInvocationHandler(Class<T> proxyClass, F proxyFactory, Set<Method> readMethods, Set<Method> writeMethods)
	{
		super(proxyClass, proxyFactory, null);
		
		this.freeMethod = Methods.findMethod(proxyClass, "free");
		this.readMethods = readMethods;
		this.writeMethods = writeMethods;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected InvocationStrategy getInvocationStrategy(T locator, Method method, Object... parameters) throws SQLException
	{
		if (this.readMethods.contains(method))
		{
			return this.getProxyFactory().locatorsUpdateCopy() ? InvocationStrategies.INVOKE_ON_ANY : InvocationStrategies.INVOKE_ON_NEXT;
		}
		
		if (this.writeMethods.contains(method))
		{
			return this.getProxyFactory().locatorsUpdateCopy() ? InvocationStrategies.INVOKE_ON_EXISTING : InvocationStrategies.INVOKE_ON_ALL;
		}
		
		return super.getInvocationStrategy(locator, method, parameters);
	}
	
	@Override
	protected <R> void postInvoke(Invoker<Z, D, T, R, SQLException> invoker, T proxy, Method method, Object... parameters)
	{
		super.postInvoke(invoker, proxy, method, parameters);
		
		if ((this.freeMethod != null) && method.equals(this.freeMethod))
		{
			this.getProxyFactory().remove();
		}
	}
}
