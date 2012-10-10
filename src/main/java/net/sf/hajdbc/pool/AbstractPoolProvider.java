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
package net.sf.hajdbc.pool;

/**
 * Abstract pool provider implementation.
 * @author Paul Ferraro
 * @param <T>
 * @param <E>
 */
public abstract class AbstractPoolProvider<T, E extends Exception> implements PoolProvider<T, E>
{
	private final Class<T> providedClass;
	private final Class<E> exceptionClass;
	
	protected AbstractPoolProvider(Class<T> providedClass, Class<E> exceptionClass)
	{
		this.providedClass = providedClass;
		this.exceptionClass = exceptionClass;
	}
	
	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.pool.PoolProvider#getExceptionClass()
	 */
	@Override
	public Class<E> getExceptionClass()
	{
		return this.exceptionClass;
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.pool.PoolProvider#getProvidedClass()
	 */
	@Override
	public Class<T> getProvidedClass()
	{
		return this.providedClass;
	}
}
