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

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * @author Paul Ferraro
 */
public class MapRegistryStoreFactory<K> implements RegistryStoreFactory<K>
{
	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.util.concurrent.RegistryStoreFactory#createStore()
	 */
	@Override
	public <V> RegistryStore<K, V> createStore()
	{
		return new MapRegistryStore<K, V>();
	}
	
	static class MapRegistryStore<K, V> implements RegistryStore<K, V>
	{
		private final ConcurrentMap<K, V> map = new ConcurrentHashMap<K, V>();
		
		/**
		 * {@inheritDoc}
		 * @see net.sf.hajdbc.util.concurrent.RegistryStore#setIfAbsent(java.lang.Object, java.lang.Object)
		 */
		@Override
		public V setIfAbsent(K key, V value)
		{
			return this.map.putIfAbsent(key, value);
		}

		/**
		 * {@inheritDoc}
		 * @see net.sf.hajdbc.util.concurrent.RegistryStore#get(java.lang.Object)
		 */
		@Override
		public V get(K key)
		{
			return this.map.get(key);
		}

		/**
		 * {@inheritDoc}
		 * @see net.sf.hajdbc.util.concurrent.RegistryStore#clear(java.lang.Object)
		 */
		@Override
		public void clear(K key)
		{
			this.map.remove(key);
		}
	}
}
