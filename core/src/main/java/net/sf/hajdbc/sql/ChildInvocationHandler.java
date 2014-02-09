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

import net.sf.hajdbc.Database;

/**
 * @author Paul Ferraro
 * @param <D> 
 * @param <P> 
 * @param <T> 
 */
public abstract class ChildInvocationHandler<Z, D extends Database<Z>, P, PE extends Exception, T, E extends Exception, F extends ChildProxyFactory<Z, D, P, PE, T, E>> extends AbstractInvocationHandler<Z, D, T, E, F>
{
	private final Method parentMethod;
	
	protected ChildInvocationHandler(Class<T> proxyClass, F proxyFactory, Method parentMethod)
	{
		super(proxyClass, proxyFactory);
		
		this.parentMethod = parentMethod;
	}

	@Override
	public Object invoke(Object proxy, Method method, Object[] args) throws Throwable
	{
		return ((this.parentMethod != null) && this.parentMethod.equals(method)) ? this.getProxyFactory().getParentProxy() : super.invoke(proxy, method, args);
	}
}
