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
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

/**
 * Dialect for <a href="firebird.sourceforge.net">Firebird</a>.
 * @author Paul Ferraro
 */
public class FirebirdDialect extends DefaultDialect
{
	/**
	 * @see net.sf.hajdbc.dialect.DefaultDialect#dummyTable()
	 */
	@Override
	protected String dummyTable()
	{
		return "RDB$DATABASE";
	}

	/**
	 * Firebird 2.0 will support standard syntax.  Until then...
	 * @see net.sf.hajdbc.dialect.DefaultDialect#alterSequenceFormat()
	 */
	@Override
	protected String alterSequenceFormat()
	{
		return "SET GENERATOR {0} TO {1}";
	}

	/**
	 * @see net.sf.hajdbc.dialect.DefaultDialect#getSequences(java.sql.Connection)
	 */
	@Override
	public Collection<String> getSequences(Connection connection) throws SQLException
	{
		List<String> sequenceList = new LinkedList<String>();
		
		Statement statement = connection.createStatement();
		
		ResultSet resultSet = statement.executeQuery("SELECT RDB$GENERATOR_NAME FROM RDB$GENERATORS");
		
		while (resultSet.next())
		{
			sequenceList.add(resultSet.getString(1));
		}
		
		resultSet.close();
		statement.close();
		
		return sequenceList;
	}

	/**
	 * @see net.sf.hajdbc.dialect.DefaultDialect#supportsAutoIncrementColumns()
	 */
	@Override
	public boolean supportsAutoIncrementColumns()
	{
		return false;
	}

	/**
	 * Firebird 2.0 will support standard syntax.  Until then...
	 * @see net.sf.hajdbc.dialect.DefaultDialect#sequencePattern()
	 */
	@Override
	protected String sequencePattern()
	{
		return "GEN_ID\\s*\\(\\s*(\\S+)\\s*,\\s*\\d+\\s*\\)";
	}

	/**
	 * @see net.sf.hajdbc.dialect.DefaultDialect#selectForUpdatePattern()
	 */
	@Override
	protected String selectForUpdatePattern()
	{
		return "SELECT\\s+.+\\s+WITH\\s+LOCK";
	}

	/**
	 * @see net.sf.hajdbc.dialect.DefaultDialect#currentSequenceValueFormat()
	 */
	@Override
	protected String currentSequenceValueFormat()
	{
		return "GEN_ID({0}, 0)";
	}
}
