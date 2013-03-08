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
import java.lang.reflect.Proxy;

/**
 * @author Paul Ferraro
 */
public final class Proxies
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
	
	private Proxies()
	{
		// Hide constructor
	}
}
