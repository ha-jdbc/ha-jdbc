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
package net.sf.hajdbc.util.concurrent;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import net.sf.hajdbc.ExceptionFactory;
import net.sf.hajdbc.Lifecycle;

/**
 * @author Paul Ferraro
 */
public class Registry<K, V extends Lifecycle, C, E extends Exception>
{
	private final RegistryStore<K, RegistryEntry> store;
	final Factory<K, V, C, E> factory;
	final ExceptionFactory<E> exceptionFactory;

	public Registry(Factory<K, V, C, E> factory, RegistryStoreFactory<K> storeFactory, ExceptionFactory<E> exceptionFactory)
	{
		this.store = storeFactory.createStore();
		this.factory = factory;
		this.exceptionFactory = exceptionFactory;
	}
	
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
			this.store.clear(key);
			
			value.stop();
			
			throw this.exceptionFactory.createException(e);
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
				try
				{
					if (!latch.await(Registry.this.factory.getTimeout(), Registry.this.factory.getTimeoutUnit()))
					{
						throw Registry.this.exceptionFactory.createException(new TimeoutException());
					}
				}
				catch (InterruptedException e)
				{
					Thread.currentThread().interrupt();
					throw Registry.this.exceptionFactory.createException(e);
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
	
	public interface Factory<K, V, C, E extends Exception>
	{
		V create(K key, C context) throws E;
		
		long getTimeout();
		
		TimeUnit getTimeoutUnit();
	}
}
