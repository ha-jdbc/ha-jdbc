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
package net.sf.hajdbc.dialect;

import java.util.Collection;
import java.util.LinkedList;

import net.sf.hajdbc.IdentifierNormalizer;

/**
 * Custom list implementation that normalizes the element entries as they are added.
 * @author Paul Ferraro
 */
public class IdentifierList extends LinkedList<String>
{
	private static final long serialVersionUID = -3743137491103447812L;

	private final IdentifierNormalizer normalizer;
	
	public IdentifierList(IdentifierNormalizer normalizer)
	{
		this.normalizer = normalizer;
	}

	@Override
	public boolean add(String id)
	{
		this.add(this.size(), id);
		return true;
	}

	@Override
	public void add(int index, String id)
	{
		super.add(index, this.normalizer.normalize(id));
	}

	@Override
	public void addFirst(String id)
	{
		this.add(0, id);
	}

	@Override
	public void addLast(String id)
	{
		this.add(id);
	}

	@Override
	public boolean addAll(Collection<? extends String> ids)
	{
		return this.addAll(this.size(), ids);
	}

	@Override
	public boolean addAll(int index, Collection<? extends String> ids)
	{
		int i = index;
		for (String id: ids)
		{
			this.add(i++, id);
		}
		return (i != index);
	}
}
