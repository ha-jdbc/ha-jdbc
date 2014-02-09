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
package net.sf.hajdbc.util;

import java.security.AccessController;
import java.security.PrivilegedAction;
import java.security.PrivilegedExceptionAction;

public class Security
{
	public static <T> T run(PrivilegedAction<T> action)
	{
		return (System.getSecurityManager() != null) ? AccessController.doPrivileged(action) : action.run();
	}

	public static <T> T run(PrivilegedExceptionAction<T> action) throws Exception
	{
		return (System.getSecurityManager() != null) ? AccessController.doPrivileged(action) : action.run();
	}

	public static <T, E extends Exception> T run(PrivilegedExceptionAction<T> action, Class<E> exceptionClass) throws E
	{
		try
		{
			return (System.getSecurityManager() != null) ? AccessController.doPrivileged(action) : action.run();
		}
		catch (Exception e)
		{
			throw exceptionClass.cast(e);
		}
	}

	private Security()
	{
		// Hide constructor
	}
}
