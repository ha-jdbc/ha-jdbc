/*
 * HA-JDBC: High-Availablity JDBC
 * Copyright 2010 Paul Ferraro
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
package net.sf.hajdbc.durability;

import java.io.Serializable;

/**
 * @author Paul Ferraro
 */
public class InvokerResultImpl implements InvokerResult, Serializable
{
	private static final long serialVersionUID = 1497455948088313742L;
	
	private final Object value;
	private final Throwable exception;
	
	public InvokerResultImpl(Object value)
	{
		this.value = value;
		this.exception = null;
	}
	
	public InvokerResultImpl(Throwable exception)
	{
		this.value = null;
		this.exception = exception;
	}
	
	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.durability.InvokerResult#getValue()
	 */
	@Override
	public Object getValue()
	{
		return this.value;
	}
	
	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.durability.InvokerResult#getException()
	 */
	@Override
	public Throwable getException()
	{
		return this.exception;
	}
}
