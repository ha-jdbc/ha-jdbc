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
package net.sf.hajdbc.util.reflect;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;

/**
 * @author Paul Ferraro
 *
 */
public final class ProxyFactory
{
	/**
	 * Helper method to simplify creation and casting of a proxy instance for a single interface.
	 * @param <T> target interface
	 * @param targetInterface interface to proxy
	 * @param handler an invocation handler
	 * @return a proxy instance for the given target interface
	 */
	public static <T> T createProxy(Class<T> targetInterface, InvocationHandler handler)
	{
		return targetInterface.cast(Proxy.newProxyInstance(targetInterface.getClassLoader(), new Class<?>[] { targetInterface }, handler));
	}
}
