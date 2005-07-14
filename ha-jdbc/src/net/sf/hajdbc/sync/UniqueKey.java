/*
 * HA-JDBC: High-Availability JDBC
 * Copyright (C) 2004 Paul Ferraro
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
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

/**
 * @author  Paul Ferraro
 * @since   1.0
 */
public class UniqueKey extends Key
{
	/** SQL-92 compatible create foreign key statement pattern */
	public static final String DEFAULT_CREATE_SQL = "ALTER TABLE {1} ADD CONSTRAINT {0} UNIQUE ({2})";

	/** SQL-92 compatible drop foreign key statement pattern */
	public static final String DEFAULT_DROP_SQL = "ALTER TABLE {1} DROP CONSTRAINT {0}";
	
	private Map columnMap = new TreeMap();
	
	/**
	 * Constructs a new UniqueKey.
	 * @param name
	 * @param table
	 * @param quote
	 */
	public UniqueKey(String name, String table, String quote)
	{
		super(name, table, quote);
	}
	
	/**
	 * @param position
	 * @param column
	 */
	public void addColumn(short position, String column)
	{
		this.columnMap.put(new Short(position), column);
	}
	
	protected String formatSQL(String pattern)
	{
		StringBuffer buffer = new StringBuffer();
		
		Iterator columns = this.columnMap.values().iterator();
		
		while (columns.hasNext())
		{
			buffer.append(columns.next());
			
			if (columns.hasNext())
			{
				buffer.append(", ");
			}
		}
		
		return MessageFormat.format(pattern, new Object[] { this.quote(this.name), this.table, buffer.toString() });
	}
	
	/**
	 * Collects all foreign keys from the specified tables using the specified connection. 
	 * @param connection a database connection
	 * @param table a table name
	 * @param primaryKeyName the name of the primary key of this table
	 * @return a Collection<ForeignKey>.
	 * @throws SQLException if a database error occurs
	 */
	public static Collection collect(Connection connection, String table, String primaryKeyName) throws SQLException
	{
		Map keyMap = new HashMap();
		DatabaseMetaData metaData = connection.getMetaData();
		String quote = metaData.getIdentifierQuoteString();
		
		ResultSet resultSet = metaData.getIndexInfo(null, null, table, true, false);
		
		while (resultSet.next())
		{
			String name = resultSet.getString("INDEX_NAME");
			
			if ((name == null) || name.equals(primaryKeyName)) continue;
			
			UniqueKey key = (UniqueKey) keyMap.get(name);
			
			if (key == null)
			{
				key = new UniqueKey(name, table, quote);
				
				keyMap.put(name, key);
			}
			
			short position = resultSet.getShort("ORDINAL_POSITION");
			String column = resultSet.getString("COLUMN_NAME");
			
			key.addColumn(position, column);
		}
		
		resultSet.close();
		
		return keyMap.values();
	}
}
