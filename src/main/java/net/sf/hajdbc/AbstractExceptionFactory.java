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
package net.sf.hajdbc;

import net.sf.hajdbc.durability.Durability.Phase;


/**
 * @author paul
 *
 */
public abstract class AbstractExceptionFactory<E extends Exception> implements ExceptionFactory<E>
{
	private static final long serialVersionUID = 6715631233424287636L;

	private Class<E> targetClass;
	
	protected AbstractExceptionFactory(Class<E> targetClass)
	{
		this.targetClass = targetClass;
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.ExceptionFactory#createException(java.lang.Throwable)
	 */
	@Override
	public E createException(Throwable e)
	{
		if (this.targetClass.isInstance(e))
		{
			return this.targetClass.cast(e);
		}
		
		E exception = this.createException(e.getMessage());
		
		exception.initCause(e);
		
		return exception;
	}

	@Override
	public boolean correctHeuristic(E exception, Phase phase)
	{
		return false;
	}

	@Override
	public boolean equals(E exception1, E exception2)
	{
		String message1 = exception1.getMessage();
		String message2 = exception2.getMessage();
		
		return ((message1 != null) && (message2 != null)) ? message1.equals(message2) : message1 == message2;
	}
}
