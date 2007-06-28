/*
 * HA-JDBC: High-Availability JDBC
 * Copyright (c) 2004-2007 Paul Ferraro
 * 
 * This library is free software; you can redistribute it and/or modify it 
 * under the terms of the GNU Lesser General Public License as published by the 
 * Free Software Foundation; either version 2.1 of the License, or (at your 
 * option) any later version.
 * 
 * This library is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or 
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public License 
 * for more details.
 * 
 * You should have received a copy of the GNU Lesser General Public License
 * along with this library; if not, write to the Free Software Foundation, 
 * Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA
 * 
 * Contact: ferraro@users.sourceforge.net
 */
package net.sf.hajdbc.cache;

import java.util.LinkedList;
import java.util.List;

import net.sf.hajdbc.UniqueConstraint;

/**
 * @author Paul Ferraro
 *
 */
public class UniqueConstraintImpl implements UniqueConstraint
{
	private String name;
	private String table;
	private List<String> columnList = new LinkedList<String>();
		
	/**
	 * Constructs a new UniqueConstraint.
	 * @param name the name of this constraint
	 * @param table a schema qualified table name
	 */
	public UniqueConstraintImpl(String name, String table)
	{
		this.name = name;
		this.table = table;
	}
	
	/**
	 * @return the list of columns in this unique constraint
	 */
	public List<String> getColumnList()
	{
		return this.columnList;
	}
	
	/**
	 * @return the name of this constraint
	 */
	public String getName()
	{
		return this.name;
	}

	/**
	 * @return the table of this constraint
	 */
	public String getTable()
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
	public int compareTo(UniqueConstraint constraint)
	{
		return this.name.compareTo(constraint.getName());
	}
}
