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
import java.text.MessageFormat;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * @author  Paul Ferraro
 * @version $Revision$
 * @since   1.0
 */
public class ForeignKey extends Key
{
	/** SQL-92 compatible create foreign key statement pattern */
	public static final String DEFAULT_CREATE_SQL = "ALTER TABLE {1} ADD CONSTRAINT {0} FOREIGN KEY ({2}) REFERENCES {3} ({4})";

	/** SQL-92 compatible drop foreign key statement pattern */
	public static final String DEFAULT_DROP_SQL = "ALTER TABLE {1} DROP CONSTRAINT {0}";
	
	private String column;
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
	 * @param quote
	 */
	public ForeignKey(String name, String schema, String table, String column, String foreignSchema, String foreignTable, String foreignColumn, String quote)
	{
		super(name, schema, table, quote);
		
		this.column = quote + column + quote;

		StringBuilder builder = new StringBuilder();
		
		if (foreignSchema != null)
		{
			builder.append(quote).append(foreignSchema).append(quote).append(".");
		}
		
		this.foreignTable = builder.append(quote).append(foreignTable).append(quote).toString();
		this.foreignColumn = quote + foreignColumn + quote;
	}
	
	protected String formatSQL(String pattern)
	{
		return MessageFormat.format(pattern, this.name, this.table, this.column, this.foreignTable, this.foreignColumn);
	}
	
	/**
	 * Collects all foreign keys from the specified tables using the specified connection. 
	 * @param connection a database connection
	 * @param schemaMap a map of schema name to list of table names
	 * @return a Collection of ForeignKey objects.
	 * @throws SQLException if a database error occurs
	 */
	public static Collection collect(Connection connection, Map<String, List<String>> schemaMap) throws SQLException
	{
		List<ForeignKey> foreignKeyList = new LinkedList<ForeignKey>();
		DatabaseMetaData metaData = connection.getMetaData();
		String quote = metaData.getIdentifierQuoteString();
		
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
		
					ForeignKey key = new ForeignKey(name, schema, table, column, foreignSchema, foreignTable, foreignColumn, quote);
					
					foreignKeyList.add(key);
				}
				
				resultSet.close();
			}
		}
		
		return foreignKeyList;
	}
}
