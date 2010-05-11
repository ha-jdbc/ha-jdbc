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
 * Dialect for <a href="firebird.sourceforge.net">Firebird</a>.
 * @author Paul Ferraro
 */
@SuppressWarnings("nls")
public class FirebirdDialect extends StandardDialect
{
	/**
	 * @see net.sf.hajdbc.dialect.StandardDialect#dummyTable()
	 */
	@Override
	protected String dummyTable()
	{
		return "RDB$DATABASE";
	}

	/**
	 * Firebird 2.0 will support standard syntax.  Until then...
	 * @see net.sf.hajdbc.dialect.StandardDialect#alterSequenceFormat()
	 */
	@Override
	protected String alterSequenceFormat()
	{
		return "SET GENERATOR {0} TO {1}";
	}

	/**
	 * @see net.sf.hajdbc.dialect.StandardDialect#getSequences(java.sql.DatabaseMetaData)
	 */
	@Override
	public Map<QualifiedName, Integer> getSequences(DatabaseMetaData metaData) throws SQLException
	{
		Map<QualifiedName, Integer> sequences = new HashMap<QualifiedName, Integer>();
		
		Statement statement = metaData.getConnection().createStatement();
		
		ResultSet resultSet = statement.executeQuery("SELECT RDB$GENERATOR_NAME FROM RDB$GENERATORS");
		
		while (resultSet.next())
		{
			sequences.put(new QualifiedName(resultSet.getString(1)), 1);
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
	 * Firebird 2.0 will support standard syntax.  Until then...
	 * @see net.sf.hajdbc.dialect.StandardDialect#sequencePattern()
	 */
	@Override
	protected String sequencePattern()
	{
		return "GEN_ID\\s*\\(\\s*([^\\s,]+)\\s*,\\s*\\d+\\s*\\)";
	}

	/**
	 * @see net.sf.hajdbc.dialect.StandardDialect#selectForUpdatePattern()
	 */
	@Override
	protected String selectForUpdatePattern()
	{
		return "SELECT\\s+.+\\s+WITH\\s+LOCK";
	}

	/**
	 * @see net.sf.hajdbc.dialect.StandardDialect#nextSequenceValueFormat()
	 */
	@Override
	protected String nextSequenceValueFormat()
	{
		return "GEN_ID({0}, 1)";
	}
}
