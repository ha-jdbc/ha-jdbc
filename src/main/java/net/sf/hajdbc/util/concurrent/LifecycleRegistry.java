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
package net.sf.hajdbc.util.concurrent;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import net.sf.hajdbc.ExceptionFactory;
import net.sf.hajdbc.Lifecycle;
import net.sf.hajdbc.logging.Level;
import net.sf.hajdbc.logging.Logger;
import net.sf.hajdbc.logging.LoggerFactory;
import net.sf.hajdbc.util.TimePeriod;

/**
 * @author Paul Ferraro
 */
public class LifecycleRegistry<K, V extends Lifecycle, C, E extends Exception> implements Registry<K, V, C, E>
{
	private final Logger logger = LoggerFactory.getLogger(this.getClass());	
	private final LifecycleRegistry.Store<K, RegistryEntry> store;
	
	final Factory<K, V, C, E> factory;
	final ExceptionFactory<E> exceptionFactory;

	public LifecycleRegistry(Factory<K, V, C, E> factory, RegistryStoreFactory<K> storeFactory, ExceptionFactory<E> exceptionFactory)
	{
		this.store = storeFactory.createStore();
		this.factory = factory;
		this.exceptionFactory = exceptionFactory;
	}
	
	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.util.concurrent.Registry#get(java.lang.Object, java.lang.Object)
	 */
	@Override
	public V get(K key, C context) throws E
	{
		RegistryEntry entry = this.store.get(key);
		
		if (entry != null)
		{
			return entry.getValue();
		}

		V value = this.factory.create(key, context);
		
		entry = new RegistryEntry(value);

		RegistryEntry existing = this.store.setIfAbsent(key, entry);
		
		if (existing != null)
		{
			return existing.getValue();
		}
		
		try
		{
			value.start();
			
			entry.started();
			
			return value;
		}
		catch (Exception e)
		{
			try
			{
				value.stop();
			}
			catch (Exception re)
			{
				this.logger.log(Level.INFO, re);
			}
			
			this.store.clear(key);
			
			throw this.exceptionFactory.createException(e);
		}
	}
	
	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.util.concurrent.Registry#remove(java.lang.Object)
	 */
	@Override
	public void remove(K key) throws E
	{
		RegistryEntry entry = this.store.clear(key);
		
		if (entry != null)
		{
			entry.getValue().stop();
		}
	}
	
	private class RegistryEntry
	{
		private final V value;
		private final AtomicReference<CountDownLatch> latchRef = new AtomicReference<CountDownLatch>(new CountDownLatch(1));
		
		RegistryEntry(V value)
		{
			this.value = value;
		}
		
		V getValue() throws E
		{
			CountDownLatch latch = this.latchRef.get();
			
			if (latch != null)
			{
				TimePeriod timeout = LifecycleRegistry.this.factory.getTimeout();
				try
				{
					if (!latch.await(timeout.getValue(), timeout.getUnit()))
					{
						throw LifecycleRegistry.this.exceptionFactory.createException(new TimeoutException());
					}
				}
				catch (InterruptedException e)
				{
					Thread.currentThread().interrupt();
					throw LifecycleRegistry.this.exceptionFactory.createException(e);
				}
			}
			
			return this.value;
		}

		void started()
		{
			CountDownLatch latch = this.latchRef.getAndSet(null);
			
			if (latch != null)
			{
				latch.countDown();
			}
		}
	}
}
