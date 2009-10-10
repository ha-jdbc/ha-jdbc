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


/**
 * @author Paul Ferraro
 */
public class ForeignKeyConstraintImpl extends UniqueConstraintImpl implements ForeignKeyConstraint
{
	private String foreignTable;
	private List<String> foreignColumnList = new LinkedList<String>();
	private int updateRule;
	private int deleteRule;
	private int deferrability;
	
	/**
	 * Constructs a new ForeignKey.
	 * @param name the name of this constraint
	 * @param table a schema qualified table name
	 */
	public ForeignKeyConstraintImpl(String name, String table)
	{
		super(name, table);
	}
	
	/**
	 * @see net.sf.hajdbc.cache.ForeignKeyConstraint#getForeignTable()
	 */
	@Override
	public String getForeignTable()
	{
		return this.foreignTable;
	}
	
	/**
	 * @see net.sf.hajdbc.cache.ForeignKeyConstraint#getForeignColumnList()
	 */
	@Override
	public List<String> getForeignColumnList()
	{
		return this.foreignColumnList;
	}
	
	/**
	 * @see net.sf.hajdbc.cache.ForeignKeyConstraint#getDeleteRule()
	 */
	@Override
	public int getDeleteRule()
	{
		return this.deleteRule;
	}

	/**
	 * @see net.sf.hajdbc.cache.ForeignKeyConstraint#getUpdateRule()
	 */
	@Override
	public int getUpdateRule()
	{
		return this.updateRule;
	}

	/**
	 * @see net.sf.hajdbc.cache.ForeignKeyConstraint#getDeferrability()
	 */
	@Override
	public int getDeferrability()
	{
		return this.deferrability;
	}

	/**
	 * @see net.sf.hajdbc.cache.ForeignKeyConstraint#setDeferrability(int)
	 */
	@Override
	public void setDeferrability(int deferrability)
	{
		this.deferrability = deferrability;
	}

	/**
	 * @see net.sf.hajdbc.cache.ForeignKeyConstraint#setDeleteRule(int)
	 */
	@Override
	public void setDeleteRule(int deleteRule)
	{
		this.deleteRule = deleteRule;
	}

	/**
	 * @see net.sf.hajdbc.cache.ForeignKeyConstraint#setForeignTable(java.lang.String)
	 */
	@Override
	public void setForeignTable(String foreignTable)
	{
		this.foreignTable = foreignTable;
	}

	/**
	 * @see net.sf.hajdbc.cache.ForeignKeyConstraint#setUpdateRule(int)
	 */
	@Override
	public void setUpdateRule(int updateRule)
	{
		this.updateRule = updateRule;
	}
}
