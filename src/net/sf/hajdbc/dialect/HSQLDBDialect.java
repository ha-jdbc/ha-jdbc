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
import java.util.HashMap;
import java.util.Map;

/**
 * Dialect for <a href="http://www.hsqldb.org">HSQLDB</a>.
 * 
 * @author  Paul Ferraro
 * @since   1.1
 */
public class HSQLDBDialect extends DefaultDialect
{
	/**
	 * @see net.sf.hajdbc.dialect.DefaultDialect#getSimpleSQL()
	 */
	@Override
	public String getSimpleSQL()
	{
		return "CALL NOW()";
	}
	
	/**
	 * @see net.sf.hajdbc.dialect.DefaultDialect#getSequences(java.sql.Connection)
	 */
	@Override
	public Map<String, Long> getSequences(Connection connection) throws SQLException
	{
		Map<String, Long> sequenceMap = new HashMap<String, Long>();
		
		ResultSet resultSet = connection.createStatement().executeQuery("SELECT SEQUENCE_SCHEMA, SEQUENCE_NAME, NEXT_VALUE FROM INFORMATION_SCHEMA.SYSTEM_SEQUENCES");
		
		while (resultSet.next())
		{
			StringBuilder builder = new StringBuilder();
			
			String schema = resultSet.getString(1);
			
			if (schema != null)
			{
				builder.append(schema).append(".");
			}
			
			sequenceMap.put(builder.append(resultSet.getString(2)).toString(), resultSet.getLong(3));
		}
		
		resultSet.getStatement().close();
		
		return sequenceMap;
	}

	/**
	 * Deferrability clause is not supported.
	 * @see net.sf.hajdbc.dialect.DefaultDialect#createForeignKeyFormat()
	 */
	@Override
	protected String createForeignKeyFormat()
	{
		return "ALTER TABLE {1} ADD CONSTRAINT {0} FOREIGN KEY ({2}) REFERENCES {3} ({4}) ON DELETE {5,choice,0#CASCADE|1#RESTRICT|2#SET NULL|3#NO ACTION|4#SET DEFAULT} ON UPDATE {6,choice,0#CASCADE|1#RESTRICT|2#SET NULL|3#NO ACTION|4#SET DEFAULT}";
	}
}
