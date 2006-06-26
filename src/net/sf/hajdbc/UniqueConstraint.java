/*
 * HA-JDBC: High-Availability JDBC
 * Copyright (c) 2004-2006 Paul Ferraro
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
package net.sf.hajdbc;

import java.util.LinkedList;
import java.util.List;

/**
 * Represents a unique constraint on a table.
 * 
 * @author  Paul Ferraro
 * @since   1.1
 */
public class UniqueConstraint implements Comparable<UniqueConstraint>
{
	private String name;
	private String table;
	private List<String> columnList = new LinkedList<String>();
		
	/**
	 * Constructs a new UniqueConstraint.
	 * @param name
	 * @param schema
	 * @param table
	 */
	public UniqueConstraint(String name, String table)
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
		UniqueConstraint key = (UniqueConstraint) object;
		
		return (key != null) && (key.name != null) && key.name.equals(this.name);
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
	 * @see java.lang.Comparable#compareTo(T)
	 */
	public int compareTo(UniqueConstraint constraint)
	{
		return this.name.compareTo(constraint.name);
	}
}
