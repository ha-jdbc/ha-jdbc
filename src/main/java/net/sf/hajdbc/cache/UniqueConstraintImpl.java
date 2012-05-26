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
package net.sf.hajdbc.cache;

import java.util.LinkedList;
import java.util.List;

import net.sf.hajdbc.QualifiedName;
import net.sf.hajdbc.UniqueConstraint;


/**
 * @author Paul Ferraro
 *
 */
public class UniqueConstraintImpl implements UniqueConstraint
{
	private String name;
	private QualifiedName table;
	private List<String> columnList = new LinkedList<String>();
		
	/**
	 * Constructs a new UniqueConstraint.
	 * @param name the name of this constraint
	 * @param table a schema qualified table name
	 */
	public UniqueConstraintImpl(String name, QualifiedName table)
	{
		this.name = name;
		this.table = table;
	}
	
	/**
	 * @see net.sf.hajdbc.UniqueConstraint#getColumnList()
	 */
	@Override
	public List<String> getColumnList()
	{
		return this.columnList;
	}
	
	/**
	 * @see net.sf.hajdbc.UniqueConstraint#getName()
	 */
	@Override
	public String getName()
	{
		return this.name;
	}

	/**
	 * @see net.sf.hajdbc.UniqueConstraint#getTable()
	 */
	@Override
	public QualifiedName getTable()
	{
		return this.table;
	}
	
	/**
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object object)
	{
		if ((object == null) || !(object instanceof UniqueConstraint)) return false;
		
		String name = ((UniqueConstraint) object).getName();
		
		return (name != null) && name.equals(this.name);
	}
	
	/**
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode()
	{
		return this.name.hashCode();
	}

	/**
	 * @see java.lang.Comparable#compareTo(java.lang.Object)
	 */
	@Override
	public int compareTo(UniqueConstraint constraint)
	{
		return this.name.compareTo(constraint.getName());
	}
}
