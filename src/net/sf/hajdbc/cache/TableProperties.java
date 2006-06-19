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
package net.sf.hajdbc.cache;

import java.util.Collection;
import java.util.Map;

import net.sf.hajdbc.ForeignKeyConstraint;
import net.sf.hajdbc.UniqueConstraint;

/**
 * @author Paul Ferraro
 *
 */
public class TableProperties
{
	private UniqueConstraint primaryKey;
	private Collection<ForeignKeyConstraint> foreignKeyConstraints;
	private Collection<UniqueConstraint> uniqueConstraints;
	private Map<String, ColumnProperties> columnMap;
	private String qualifiedTableForDDL;
	private String qualifiedTableForDML;
	
	/**
	 * @return Returns the columns.
	 */
	public Map<String, ColumnProperties> getColumnMap()
	{
		return this.columnMap;
	}
	
	/**
	 * @param columns The columns to set.
	 */
	public void setColumns(Map<String, ColumnProperties> columnMap)
	{
		this.columnMap = columnMap;
	}
	
	/**
	 * @return Returns the foreignKeyConstraints.
	 */
	public Collection<ForeignKeyConstraint> getForeignKeyConstraints()
	{
		return this.foreignKeyConstraints;
	}
	
	/**
	 * @param foreignKeyConstraints The foreignKeyConstraints to set.
	 */
	public void setForeignKeyConstraints(Collection<ForeignKeyConstraint> foreignKeyConstraints)
	{
		this.foreignKeyConstraints = foreignKeyConstraints;
	}
	
	/**
	 * @return Returns the primaryKey.
	 */
	public UniqueConstraint getPrimaryKey()
	{
		return this.primaryKey;
	}
	
	/**
	 * @param primaryKey The primaryKey to set.
	 */
	public void setPrimaryKey(UniqueConstraint primaryKey)
	{
		this.primaryKey = primaryKey;
	}
	
	/**
	 * @return Returns the qualifiedTableForDDL.
	 */
	public String getQualifiedTableForDDL()
	{
		return this.qualifiedTableForDDL;
	}
	
	/**
	 * @param qualifiedTableForDDL The qualifiedTableForDDL to set.
	 */
	public void setQualifiedTableForDDL(String qualifiedTableForDDL)
	{
		this.qualifiedTableForDDL = qualifiedTableForDDL;
	}
	
	/**
	 * @return Returns the qualifiedTableForDML.
	 */
	public String getQualifiedTableForDML()
	{
		return this.qualifiedTableForDML;
	}
	
	/**
	 * @param qualifiedTableForDML The qualifiedTableForDML to set.
	 */
	public void setQualifiedTableForDML(String qualifiedTableForDML)
	{
		this.qualifiedTableForDML = qualifiedTableForDML;
	}
	
	/**
	 * @return Returns the uniqueConstraints.
	 */
	public Collection<UniqueConstraint> getUniqueConstraints()
	{
		return this.uniqueConstraints;
	}
	
	/**
	 * @param uniqueConstraints The uniqueConstraints to set.
	 */
	public void setUniqueConstraints(Collection<UniqueConstraint> uniqueConstraints)
	{
		this.uniqueConstraints = uniqueConstraints;
	}		
}
