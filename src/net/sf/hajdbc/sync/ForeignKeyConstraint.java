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
package net.sf.hajdbc.sync;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Represents a foreign key constraint on a table.
 * 
 * @author  Paul Ferraro
 * @since   1.1
 */
public class ForeignKeyConstraint extends Constraint
{
	/** SQL-92 compatible create foreign key statement pattern */
	public static final String DEFAULT_CREATE_SQL = "ALTER TABLE {1} ADD CONSTRAINT {0} FOREIGN KEY ({2}) REFERENCES {3} ({4})";

	/** SQL-92 compatible drop foreign key statement pattern */
	public static final String DEFAULT_DROP_SQL = "ALTER TABLE {1} DROP CONSTRAINT {0}";
	
	private String column;
	private String foreignSchema;
	private String foreignTable;
	private String foreignColumn;
	
	/**
	 * Constructs a new ForeignKey.
	 * @param name
	 * @param schema
	 * @param table
	 * @param column
	 * @param foreignSchema
	 * @param foreignTable
	 * @param foreignColumn
	 */
	public ForeignKeyConstraint(String name, String schema, String table, String column, String foreignSchema, String foreignTable, String foreignColumn)
	{
		super(name, schema, table);
		
		this.column = column;
		this.foreignSchema = foreignSchema;
		this.foreignTable = foreignTable;
		this.foreignColumn = foreignColumn;
	}
	
	/**
	 * @return the column of this foreign key
	 */
	public String getColumn()
	{
		return this.column;
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
	public String getForeignColumn()
	{
		return this.foreignColumn;
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
		List<ForeignKeyConstraint> foreignKeyList = new LinkedList<ForeignKeyConstraint>();
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
					String column = resultSet.getString("FKCOLUMN_NAME");
					String foreignSchema = resultSet.getString("PKTABLE_SCHEM");
					String foreignTable = resultSet.getString("PKTABLE_NAME");
					String foreignColumn = resultSet.getString("PKCOLUMN_NAME");
		
					ForeignKeyConstraint key = new ForeignKeyConstraint(name, schema, table, column, foreignSchema, foreignTable, foreignColumn);
					
					foreignKeyList.add(key);
				}
				
				resultSet.close();
			}
		}
		
		return foreignKeyList;
	}
}
