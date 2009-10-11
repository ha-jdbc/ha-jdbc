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

import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;

import net.sf.hajdbc.pool.Pool;
import net.sf.hajdbc.pool.PoolProvider;

/**
 * @author paul
 *
 */
public class SimplePool<T, E extends Exception> implements Pool<T, E>
{
	private final SimplePoolConfiguration configuration;
	private final PoolProvider<T, E> provider;
	private final Queue<T> queue;

	public SimplePool(PoolProvider<T, E> provider, SimplePoolConfiguration configuration)
	{
		this.provider = provider;
		this.configuration = configuration;
		
		int capacity = configuration.getCapacity();
		
		this.queue = (capacity > 0) ? new LinkedBlockingQueue<T>(capacity) : null;
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.pool.Pool#close()
	 */
	@Override
	public void close()
	{
		if (this.queue != null)
		{
			T item = this.queue.poll();
			
			while (item != null)
			{
				this.provider.close(item);
				
				item = this.queue.poll();
			}
		}
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.pool.Pool#init()
	 */
	@Override
	public void init() throws E
	{
		if (this.queue != null)
		{
			// Fill pool to initial size
			for (int i = 0; i < this.configuration.getInitSize(); ++i)
			{
				this.queue.add(this.provider.create());
			}
		}
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.pool.Pool#release(java.lang.Object)
	 */
	@Override
	public void release(T item)
	{
		// If item cannot be released to pool, close it
		if ((this.queue == null) || !this.queue.offer(item))
		{
			this.provider.close(item);
		}
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.pool.Pool#take()
	 */
	@Override
	public T take() throws E
	{
		T item = (this.queue != null) ? (this.configuration.isEnforceMax() ? this.queue.remove() : this.queue.poll()) : null;
		
		while (item != null)
		{
			// If the item is invalid, close it and take another from the pool
			if (!this.provider.isValid(item))
			{
				this.provider.close(item);
				
				item = this.queue.poll();
			}
		}
		
		// If the pool is empty, pay the cost of item creation
		return (item != null) ? item : this.provider.create();
	}
}
