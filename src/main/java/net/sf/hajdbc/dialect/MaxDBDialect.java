/*
 * HA-JDBC: High-Availability JDBC
 * Copyright 2004-2009 Paul Ferraro
 * 
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 * 
 * You should have received a copy of the GNU Lesser General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package net.sf.hajdbc.dialect;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

import net.sf.hajdbc.cache.QualifiedName;

/**
 * Dialect for <a href="http://www.mysql.com/products/database/maxdb/">MySQL MaxDB</a>.
 * @author  Paul Ferraro
 * @since   1.1
 */
@SuppressWarnings("nls")
public class MaxDBDialect extends StandardDialect
{
	private static final long serialVersionUID = 335911716663453529L;

	/**
	 * @see net.sf.hajdbc.dialect.StandardDialect#dummyTable()
	 */
	@Override
	protected String dummyTable()
	{
		return "DUAL";
	}

	/**
	 * @see net.sf.hajdbc.dialect.StandardDialect#currentTimestampFunction()
	 */
	@Override
	protected String currentTimestampFunction()
	{
		return "SYSDATE";
	}

	/**
	 * @see net.sf.hajdbc.dialect.StandardDialect#getSequences(java.sql.DatabaseMetaData)
	 */
	@Override
	public Map<QualifiedName, Integer> getSequences(DatabaseMetaData metaData) throws SQLException
	{
		Map<QualifiedName, Integer> sequences = new HashMap<QualifiedName, Integer>();
		
		Statement statement = metaData.getConnection().createStatement();
		
		ResultSet resultSet = statement.executeQuery("SELECT SEQUENCE_NAME, INCREMENT_BY FROM USER_SEQUENCES");
		
		while (resultSet.next())
		{
			sequences.put(new QualifiedName(resultSet.getString(1)), resultSet.getInt(2));
		}
		
		statement.close();
		
		return sequences;
	}

	/**
	 * @see net.sf.hajdbc.dialect.StandardDialect#parseInsertTable(java.lang.String)
	 */
	@Override
	public String parseInsertTable(String sql)
	{
		return null;
	}

	/**
	 * @see net.sf.hajdbc.dialect.StandardDialect#truncateTableFormat()
	 */
	@Override
	protected String truncateTableFormat()
	{
		return "TRUNCATE TABLE {0}";
	}
	
	/**
	 * ON UPDATE and deferrability clauses are not supported.
	 * @see net.sf.hajdbc.dialect.StandardDialect#createForeignKeyConstraintFormat()
	 */
	@Override
	protected String createForeignKeyConstraintFormat()
	{
		return "ALTER TABLE {1} ADD CONSTRAINT {0} FOREIGN KEY ({2}) REFERENCES {3} ({4}) ON DELETE {5,choice,0#CASCADE|1#RESTRICT|2#SET NULL|3#NO ACTION|4#SET DEFAULT}";
	}

	/**
	 * @see net.sf.hajdbc.dialect.StandardDialect#sequencePattern()
	 */
	@Override
	protected String sequencePattern()
	{
		return "'?(\\w+)'?\\.(?:CURR|NEXT)VAL";
	}

	/**
	 * @see net.sf.hajdbc.dialect.StandardDialect#nextSequenceValueFormat()
	 */
	@Override
	protected String nextSequenceValueFormat()
	{
		return "{0}.NEXTVAL";
	}
}
