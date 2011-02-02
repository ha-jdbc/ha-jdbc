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

import java.util.concurrent.atomic.AtomicReference;

/**
 * @author Paul Ferraro
 */
public class ReferenceRegistryStoreFactory implements RegistryStoreFactory<Void>
{
	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.util.concurrent.RegistryStoreFactory#createStore()
	 */
	@Override
	public <V> Registry.Store<Void, V> createStore()
	{
		return new ReferenceRegistryStore<V>();
	}
	
	static class ReferenceRegistryStore<V> implements Registry.Store<Void, V>
	{
		private final AtomicReference<V> reference = new AtomicReference<V>();
		
		/**
		 * {@inheritDoc}
		 * @see net.sf.hajdbc.util.concurrent.RegistryStore#setIfAbsent(java.lang.Object, java.lang.Object)
		 */
		@Override
		public V setIfAbsent(Void key, V value)
		{
			return this.reference.compareAndSet(null, value) ? null : this.reference.get();
		}

		/**
		 * {@inheritDoc}
		 * @see net.sf.hajdbc.util.concurrent.RegistryStore#get(java.lang.Object)
		 */
		@Override
		public V get(Void key)
		{
			return this.reference.get();
		}

		/**
		 * {@inheritDoc}
		 * @see net.sf.hajdbc.util.concurrent.RegistryStore#clear(java.lang.Object)
		 */
		@Override
		public V clear(Void key)
		{
			return this.reference.getAndSet(null);
		}
	}
}
