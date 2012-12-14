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
package net.sf.hajdbc.util;

import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;

/**
 * An iterable facade for a list whose iterator operates in reverse order.
 * @author Paul Ferraro
 */
public class Reversed<T> implements Iterable<T>
{
	private final List<T> list;

	public Reversed(List<T> list)
	{
		this.list = list;
	}

	/**
	 * {@inheritDoc}
	 * @see java.lang.Iterable#iterator()
	 */
	@Override
	public Iterator<T> iterator()
	{
		final ListIterator<T> iterator = this.list.listIterator(this.list.size());
		
		return new Iterator<T>()
		{
			@Override
			public boolean hasNext()
			{
				return iterator.hasPrevious();
			}
			
			@Override
			public T next()
			{
				return iterator.previous();
			}
			
			@Override
			public void remove()
			{
				iterator.remove();
			}
		};
	}
}
