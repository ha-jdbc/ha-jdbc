/*
 * HA-JDBC: High-Availability JDBC
 * Copyright (c) 2004-2009 Paul Ferraro
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
package net.sf.hajdbc.sql;

import net.sf.hajdbc.ExceptionFactory;

/**
 * @author paul
 *
 */
public abstract class AbstractExceptionFactory<E extends Exception> implements ExceptionFactory<E>
{
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
		
		E exception = this.createException();
		
		exception.initCause(e);
		
		return exception;
	}
	
	protected abstract E createException();
}
