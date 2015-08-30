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

import java.time.Duration;

/**
 * @author Paul Ferraro
 * @param <K>
 * @param <V>
 * @param <C>
 * @param <E>
 */
public interface Registry<K, V, C, E extends Exception>
{
	V get(K key, C context) throws E;

	void remove(K key) throws E;
	
	public interface Factory<K, V, C, E extends Exception>
	{
		V create(K key, C context) throws E;
		
		Duration getTimeout();
	}
	
	public interface Store<K, V>
	{
		V setIfAbsent(K key, V value);
		
		V get(K key);
		
		V clear(K key);
	}
}