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

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.SQLException;

import net.sf.hajdbc.util.SQLExceptionFactory;

/**
 * @author Paul Ferraro
 */
public final class Methods
{
	/**
	 * Helper method for <code>Method.invoke(Object, Object...)</code> that performs the necessary exception handling and throws an SQLException.
	 * @param method a method to invoke
	 * @param object the object on which to invoke the given method
	 * @param parameters the method parameters
	 * @return the return value of the method invocation
	 * @throws SQLException the target exception of the method invocation
	 * @throws IllegalArgumentException if the the underlying method is inaccessible
	 */
	public static Object invoke(Method method, Object object, Object... parameters) throws SQLException
	{
		try
		{
			return method.invoke(object, parameters);
		}
		catch (IllegalAccessException e)
		{
			throw new IllegalArgumentException(e);
		}
		catch (InvocationTargetException e)
		{
			throw SQLExceptionFactory.createSQLException(e.getTargetException());
		}
	}
}
