/*
 * HA-JDBC: High-Availability JDBC
 * Copyright (c) 2004-2007 Paul Ferraro
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
import java.sql.SQLException;
import java.util.Collection;

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
	 * @see net.sf.hajdbc.dialect.StandardDialect#getSequences(java.sql.Connection)
	 */
	@Override
	public Collection<String> getSequences(Connection connection) throws SQLException
	{
		return this.executeQuery(connection, "SELECT RDB$GENERATOR_NAME FROM RDB$GENERATORS");
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
