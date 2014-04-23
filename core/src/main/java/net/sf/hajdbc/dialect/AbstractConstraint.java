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

import java.util.List;

import net.sf.hajdbc.AbstractNamed;
import net.sf.hajdbc.Constraint;
import net.sf.hajdbc.QualifiedName;

public abstract class AbstractConstraint<C extends Constraint<C>> extends AbstractNamed<String, C> implements Constraint<C>
{
	private final QualifiedName table;
	private final List<String> columns;

	protected AbstractConstraint(String name, QualifiedName table, List<String> columns)
	{
		super(name);
		this.table = table;
		this.columns = columns;
	}
	
	/**
	 * @see net.sf.hajdbc.UniqueConstraint#getColumnList()
	 */
	@Override
	public List<String> getColumnList()
	{
		return this.columns;
	}

	/**
	 * @see net.sf.hajdbc.UniqueConstraint#getTable()
	 */
	@Override
	public QualifiedName getTable()
	{
		return this.table;
	}
}
