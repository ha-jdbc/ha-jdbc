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
 * Helper class that houses processed database meta data for an individual table.
 * 
 * @author Paul Ferraro
 * @since 1.2
 */
public class TableProperties
{
	private String schema;
	private String table;
	private UniqueConstraint primaryKey;
	private Collection<ForeignKeyConstraint> foreignKeyConstraints;
	private Collection<UniqueConstraint> uniqueConstraints;
	private Map<String, ColumnProperties> columnMap;
	private String qualifiedNameForDDL;
	private String qualifiedNameForDML;
	
	public TableProperties(String schema, String table)
	{
		this.schema = schema;
		this.table = table;
	}
	
	/**
	 * Returns the name of this table
	 * @return the table name
	 */
	public String getTable()
	{
		return this.table;
	}
	
	/**
	 * Returns the schema of this table
	 * @return the schema name
	 */
	public String getSchema()
	{
		return this.schema;
	}
	
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
	public String getQualifiedNameForDDL()
	{
		return this.qualifiedNameForDDL;
	}
	
	/**
	 * @param qualifiedTableForDDL The qualifiedTableForDDL to set.
	 */
	public void setQualifiedNameForDDL(String qualifiedTableForDDL)
	{
		this.qualifiedNameForDDL = qualifiedTableForDDL;
	}
	
	/**
	 * @return Returns the qualifiedTableForDML.
	 */
	public String getQualifiedNameForDML()
	{
		return this.qualifiedNameForDML;
	}
	
	/**
	 * @param qualifiedTableForDML The qualifiedTableForDML to set.
	 */
	public void setQualifiedNameForDML(String qualifiedTableForDML)
	{
		this.qualifiedNameForDML = qualifiedTableForDML;
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
