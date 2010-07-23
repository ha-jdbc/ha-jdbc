/*
 * HA-JDBC: High-Availablity JDBC
 * Copyright 2004-May 24, 2010 Paul Ferraro
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
package net.sf.hajdbc;

import java.sql.SQLException;

import javax.transaction.xa.XAException;

import net.sf.hajdbc.sql.SQLExceptionFactory;
import net.sf.hajdbc.sql.xa.XAExceptionFactory;

/**
 * @author Paul Ferraro
 */
public enum ExceptionType
{
	SQL(SQLException.class, new SQLExceptionFactory()), XA(XAException.class, new XAExceptionFactory());
	
	private final Class<? extends Exception> exceptionClass;
	private final ExceptionFactory<? extends Exception> factory;
	
	private <E extends Exception> ExceptionType(Class<E> exceptionClass, ExceptionFactory<E> factory)
	{
		this.exceptionClass = exceptionClass;
		this.factory = factory;
	}
	
	@SuppressWarnings("unchecked")
	public <E extends Exception> ExceptionFactory<E> getExceptionFactory()
	{
		return (ExceptionFactory<E>) this.factory;
	}
	
	public static <E extends Exception> ExceptionFactory<E> getExceptionFactory(Class<E> exceptionClass)
	{
		for (ExceptionType type: ExceptionType.values())
		{
			if (type.exceptionClass.equals(exceptionClass))
			{
				return type.getExceptionFactory();
			}
		}
		
		throw new IllegalArgumentException(exceptionClass.getName());
	}
}