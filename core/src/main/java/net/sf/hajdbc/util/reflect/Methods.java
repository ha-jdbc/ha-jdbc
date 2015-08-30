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

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

import net.sf.hajdbc.ExceptionFactory;

/**
 * @author Paul Ferraro
 */
public final class Methods
{
	/**
	 * Helper method for <code>Method.invoke(Object, Object...)</code> that performs the necessary exception handling.
	 * @param method a method to invoke
	 * @param object the object on which to invoke the given method
	 * @param parameters the method parameters
	 * @return the return value of the method invocation
	 * @throws SQLException the target exception of the method invocation
	 * @throws IllegalArgumentException if the the underlying method is inaccessible
	 */
	@SuppressWarnings("unchecked")
	public static <R, E extends Exception> R invoke(Method method, ExceptionFactory<E> factory, Object object, Object... parameters) throws E
	{
		try
		{
			return (R) method.invoke(object, parameters);
		}
		catch (InvocationTargetException e)
		{
			throw factory.createException(e.getTargetException());
		}
		catch (IllegalAccessException e)
		{
			throw factory.createException(e);
		}
	}
	
	/**
	 * Returns a set of methods for the specified class whose names match the specified regular expression patterns.
	 * @param sourceClass the class from which to find methods
	 * @param patterns regular expression patterns
	 * @return a set of methods
	 */
	public static Set<Method> findMethods(Class<?> sourceClass, String... patterns)
	{
		List<Method> list = new LinkedList<>();
		
		Method[] methods = sourceClass.getMethods();
		
		for (String regex: patterns)
		{
			Pattern pattern = Pattern.compile(regex);
			
			for (Method method: methods)
			{
				if (pattern.matcher(method.getName()).matches())
				{
					list.add(method);
				}
			}
		}
		
		return new HashSet<>(list);
	}
	
	/**
	 * Helper method for {@link Class#getMethod(String, Class...)} where method is known to exist.
	 * @param sourceClass the class from which to find methods
	 * @param name the method name
	 * @param types the parameter types
	 * @return the method with the specified name and parameter types
	 * @throws IllegalArgumentException if no such method exists
	 */
	public static Method getMethod(Class<?> sourceClass, String name, Class<?>... types)
	{
		try
		{
			return sourceClass.getMethod(name, types);
		}
		catch (NoSuchMethodException e)
		{
			throw new IllegalArgumentException(e);
		}
	}
	
	/**
	 * Helper method for {@link Class#getMethod(String, Class...)} that returns null if the method does not exist.
	 * @param sourceClass the class from which to find methods
	 * @param name the method name
	 * @param types the parameter types
	 * @return the method with the specified name and parameter types, or null if the method does not exist
	 */
	public static Method findMethod(Class<?> sourceClass, String name, Class<?>... types)
	{
		try
		{
			return sourceClass.getMethod(name, types);
		}
		catch (NoSuchMethodException e)
		{
			return null;
		}
	}
	
	/**
	 * Helper method for {@link Class#getMethod(String, Class...)} that returns null if the class or method does not exist.
	 * @param className the name of the class containing the method
	 * @param name the method name
	 * @param types the parameter types
	 * @return the method with the specified name and parameter types, or null if the class or method does not exist
	 */
	public static Method findMethod(String className, String name, Class<?>... types)
	{
		try
		{
			return findMethod(Methods.class.getClassLoader().loadClass(className), name, types);
		}
		catch (ClassNotFoundException e)
		{
			return null;
		}
	}
	
	private Methods()
	{
		// Hide constructor
	}
}
