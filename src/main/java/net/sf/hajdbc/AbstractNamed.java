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
package net.sf.hajdbc;

/**
 * Implements {@link #hashCode()}, {@link #equals(Object)}, {@link #toString()}, and {@link #compareTo(Object)} for named objects.
 * @author Paul Ferraro
 * @param <N> the name type
 * @param <T> this type
 */
public abstract class AbstractNamed<N extends Comparable<N>, T extends Named<N, T>> implements Named<N, T>
{
	private final N name;
	
	protected AbstractNamed(N name)
	{
		this.name = name;
	}
	
	@Override
	public N getName()
	{
		return this.name;
	}

	@Override
	public int hashCode()
	{
		return this.name.hashCode();
	}

	@Override
	public boolean equals(Object object)
	{
		return ((object != null) && this.getClass().isInstance(object)) ? this.name.equals(this.getClass().cast(object).getName()) : false;
	}

	@Override
	public String toString()
	{
		return this.name.toString();
	}

	/**
	 * @see java.lang.Comparable#compareTo(java.lang.Object)
	 */
	@Override
	public int compareTo(T named)
	{
		return this.getName().compareTo(named.getName());
	}
}
