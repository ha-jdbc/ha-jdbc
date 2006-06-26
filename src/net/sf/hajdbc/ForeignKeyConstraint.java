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
 * Represents a foreign key constraint on a table.
 * 
 * @author  Paul Ferraro
 * @since   1.1
 */
public class ForeignKeyConstraint extends UniqueConstraint
{
	private String foreignTable;
	private List<String> foreignColumnList = new LinkedList<String>();
	private int updateRule;
	private int deleteRule;
	private int deferrability;
	
	/**
	 * Constructs a new ForeignKey.
	 * @param name
	 * @param schema
	 * @param table
	 */
	public ForeignKeyConstraint(String name, String table)
	{
		super(name, table);
	}
	
	/**
	 * @return the foreign table of this foreign key
	 */
	public String getForeignTable()
	{
		return this.foreignTable;
	}
	
	/**
	 * @return the foreign column of this foreign key
	 */
	public List<String> getForeignColumnList()
	{
		return this.foreignColumnList;
	}
	
	/**
	 * @return Returns the deleteRule.
	 */
	public int getDeleteRule()
	{
		return this.deleteRule;
	}

	/**
	 * @return Returns the updateRule.
	 */
	public int getUpdateRule()
	{
		return this.updateRule;
	}

	/**
	 * @return Returns the deferrability.
	 */
	public int getDeferrability()
	{
		return this.deferrability;
	}

	/**
	 * @param deferrability The deferrability to set.
	 */
	public void setDeferrability(int deferrability)
	{
		this.deferrability = deferrability;
	}

	/**
	 * @param deleteRule The deleteRule to set.
	 */
	public void setDeleteRule(int deleteRule)
	{
		this.deleteRule = deleteRule;
	}

	/**
	 * @param foreignTable The foreignTable to set.
	 */
	public void setForeignTable(String foreignTable)
	{
		this.foreignTable = foreignTable;
	}

	/**
	 * @param updateRule The updateRule to set.
	 */
	public void setUpdateRule(int updateRule)
	{
		this.updateRule = updateRule;
	}
}
