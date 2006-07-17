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

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;


/**
 * Represents a foreign key constraint on a table.
 * 
 * @author  Paul Ferraro
 * @since   1.1
 */
public class ForeignKeyConstraint extends UniqueConstraint
{
	private String foreignSchema;
	private String foreignTable;
	private List<String> foreignColumnList = new LinkedList<String>();
	private int updateRule;
	private int deleteRule;
	private int deferrability;
	
	/**
	 * Constructs a new ForeignKey.
	 * @param name the name of this constraint
	 * @param schema a schema name, possibly null
	 * @param table a table name
	 */
	public ForeignKeyConstraint(String name, String schema, String table)
	{
		super(name, schema, table);
	}
	
	/**
	 * @return the foreign table of this foreign key
	 */
	public String getForeignTable()
	{
		return this.foreignTable;
	}
	
	/**
	 * @return the foreign schema of this foreign key
	 */
	public String getForeignSchema()
	{
		return this.foreignSchema;
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
	 * @param foreignSchema The foreignSchema to set.
	 */
	public void setForeignSchema(String foreignSchema)
	{
		this.foreignSchema = foreignSchema;
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

	/**
	 * Collects all foreign keys from the specified tables using the specified connection. 
	 * @param connection a database connection
	 * @param schemaMap a map of schema name to list of table names
	 * @return a Collection of ForeignKey objects.
	 * @throws SQLException if a database error occurs
	 */
	public static Collection<ForeignKeyConstraint> collect(Connection connection, Map<String, List<String>> schemaMap) throws SQLException
	{
		Map<String, ForeignKeyConstraint> foreignKeyMap = new HashMap<String, ForeignKeyConstraint>();
		DatabaseMetaData metaData = connection.getMetaData();
		
		for (Map.Entry<String, List<String>> schemaMapEntry: schemaMap.entrySet())
		{
			String schema = schemaMapEntry.getKey();
			List<String> tableList = schemaMapEntry.getValue();
			
			for (String table: tableList)
			{
				ResultSet resultSet = metaData.getImportedKeys(null, schema, table);
				
				while (resultSet.next())
				{
					String name = resultSet.getString("FK_NAME");
					
					ForeignKeyConstraint foreignKey = foreignKeyMap.get(name);
					
					if (foreignKey == null)
					{
						foreignKey = new ForeignKeyConstraint(name, schema, table);
						
						foreignKey.foreignSchema = resultSet.getString("PKTABLE_SCHEM");
						foreignKey.foreignTable = resultSet.getString("PKTABLE_NAME");
						foreignKey.deleteRule = resultSet.getInt("DELETE_RULE");
						foreignKey.updateRule = resultSet.getInt("UPDATE_RULE");
						foreignKey.deferrability = resultSet.getInt("DEFERRABILITY");
					}
					
					String column = resultSet.getString("FKCOLUMN_NAME");
					String foreignColumn = resultSet.getString("PKCOLUMN_NAME");
		
					foreignKey.getColumnList().add(column);
					foreignKey.getForeignColumnList().add(foreignColumn);
				}
				
				resultSet.close();
			}
		}
		
		return foreignKeyMap.values();
	}
}
