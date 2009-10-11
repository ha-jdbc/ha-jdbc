/*
 * HA-JDBC: High-Availability JDBC
 * Copyright 2004-2009 Paul Ferraro
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
package net.sf.hajdbc.pool.simple;

import net.sf.hajdbc.pool.Pool;
import net.sf.hajdbc.pool.PoolFactory;
import net.sf.hajdbc.pool.PoolProvider;

/**
 * @author paul
 *
 */
public class SimplePoolFactory implements PoolFactory<SimplePoolConfiguration>
{
	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.pool.PoolFactory#createPool(net.sf.hajdbc.pool.PoolProvider)
	 */
	@Override
	public <T, E extends Exception> Pool<T, E> createPool(PoolProvider<T, E> provider, SimplePoolConfiguration configuration)
	{
		return new SimplePool<T, E>(provider, configuration);
	}
}
