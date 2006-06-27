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
package net.sf.hajdbc.dialect;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

import net.sf.hajdbc.util.Strings;

/**
 * Dialect for <a href="http://www.mysql.com/products/database/maxdb/">MySQL MaxDB</a>.
 * @author  Paul Ferraro
 * @since   1.1
 */
public class MaxDBDialect extends DefaultDialect
{
	/**
	 * @see net.sf.hajdbc.dialect.DefaultDialect#getSimpleSQL()
	 */
	@Override
	public String getSimpleSQL()
	{
		return "SELECT 1 FROM DUAL";
	}

	/**
	 * @see net.sf.hajdbc.dialect.DefaultDialect#getSequences(java.sql.Connection)
	 */
	@Override
	public Map<String, Long> getSequences(Connection connection) throws SQLException
	{
		Map<String, Long> sequenceMap = new HashMap<String, Long>();
		
		Statement statement = connection.createStatement();
		
		ResultSet resultSet = statement.executeQuery("SELECT SEQUENCE_NAME FROM ALL_SEQUENCES");
		
		while (resultSet.next())
		{
			sequenceMap.put(resultSet.getString(1), null);
		}
		
		resultSet.close();
		
		if (!sequenceMap.isEmpty())
		{
			resultSet = statement.executeQuery("SELECT " + Strings.join(sequenceMap.keySet(), ".CURRVAL, ") + ".CURRVAL FROM DUAL");
			
			resultSet.next();
			
			int index = 0;
			
			for (String sequence: sequenceMap.keySet())
			{
				sequenceMap.put(sequence, resultSet.getLong(++index));
			}
		}
		
		statement.close();
		
		return sequenceMap;
	}

	/**
	 * @see net.sf.hajdbc.dialect.DefaultDialect#truncateTableFormat()
	 */
	@Override
	protected String truncateTableFormat()
	{
		return "TRUNCATE TABLE {0}";
	}
	
	/**
	 * ON UPDATE and deferrability clauses are not supported.
	 * @see net.sf.hajdbc.dialect.DefaultDialect#createForeignKeyFormat()
	 */
	@Override
	protected String createForeignKeyFormat()
	{
		return "ALTER TABLE {1} ADD CONSTRAINT {0} FOREIGN KEY ({2}) REFERENCES {3} ({4}) ON DELETE {5,choice,0#CASCADE|1#RESTRICT|2#SET NULL|3#NO ACTION|4#SET DEFAULT}}";
	}

	/**
	 * @see net.sf.hajdbc.dialect.DefaultDialect#sequencePattern()
	 */
	@Override
	protected String sequencePattern()
	{
		return "(\\S+\\)\\.(?:(?:CURR)|(?:NEXT))VAL";
	}
}
