/*
 * HA-JDBC: High-Availability JDBC
 * Copyright (C) 2012  Paul Ferraro
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
package net.sf.hajdbc.dialect.firebird;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

import net.sf.hajdbc.SequenceProperties;
import net.sf.hajdbc.SequencePropertiesFactory;
import net.sf.hajdbc.SequenceSupport;
import net.sf.hajdbc.dialect.StandardDialect;
import net.sf.hajdbc.util.Resources;

/**
 * Dialect for <a href="firebird.sourceforge.net">Firebird</a>.
 * @author Paul Ferraro
 */
@SuppressWarnings("nls")
public class FirebirdDialect extends StandardDialect
{
	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.dialect.StandardDialect#vendorPattern()
	 */
	@Override
	protected String vendorPattern()
	{
		return "firebird";
	}

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
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.dialect.StandardDialect#getSequenceSupport()
	 */
	@Override
	public SequenceSupport getSequenceSupport()
	{
		return this;
	}

	@Override
	public Collection<SequenceProperties> getSequences(DatabaseMetaData metaData, SequencePropertiesFactory factory) throws SQLException
	{
		Statement statement = metaData.getConnection().createStatement();
		
		try
		{
			ResultSet resultSet = statement.executeQuery("SELECT RDB$GENERATOR_NAME FROM RDB$GENERATORS");
			
			List<SequenceProperties> sequences = new LinkedList<SequenceProperties>();
			
			while (resultSet.next())
			{
				sequences.add(factory.createSequenceProperties(null, resultSet.getString(1), 1));
			}
			
			return sequences;
		}
		finally
		{
			Resources.close(statement);
		}
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
