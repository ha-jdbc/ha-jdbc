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
package net.sf.hajdbc.util.ref;

/**
 * A synchronizable wrapper around a nullable object.
 * @author Paul Ferraro
 */
public class VolatileReference<T>
{
	private volatile T object;
	
	public VolatileReference()
	{
		// Do nothing
	}
	
	public VolatileReference(T object)
	{
		this.object = object;
	}
	
	public T get()
	{
		return this.object;
	}
	
	public void set(T object)
	{
		this.object = object;
	}
}
