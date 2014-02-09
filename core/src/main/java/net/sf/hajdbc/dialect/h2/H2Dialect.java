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
package net.sf.hajdbc.dialect.h2;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import net.sf.hajdbc.SequenceProperties;
import net.sf.hajdbc.SequencePropertiesFactory;
import net.sf.hajdbc.SequenceSupport;
import net.sf.hajdbc.dialect.StandardDialect;

/**
 * Dialect for <a href="http://www.h2database.com">H2 Database Engine</a>.
 * @author Paul Ferraro
 */
@SuppressWarnings("nls")
public class H2Dialect extends StandardDialect
{
	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.dialect.StandardDialect#vendorPattern()
	 */
	@Override
	protected String vendorPattern()
	{
		return "h2";
	}

	/**
	 * @see net.sf.hajdbc.dialect.StandardDialect#executeFunctionFormat()
	 */
	@Override
	protected String executeFunctionFormat()
	{
		return "CALL {0}";
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
		try (Statement statement = metaData.getConnection().createStatement())
		{
			try (ResultSet resultSet = statement.executeQuery("SELECT SEQUENCE_SCHEMA, SEQUENCE_NAME, INCREMENT FROM INFORMATION_SCHEMA.SEQUENCES"))
			{
				List<SequenceProperties> sequences = new LinkedList<>();
				
				while (resultSet.next())
				{
					sequences.add(factory.createSequenceProperties(resultSet.getString(1), resultSet.getString(2), resultSet.getInt(3)));
				}
				
				return sequences;
			}
		}
	}

	/**
	 * Deferrability clause is not supported.
	 * @see net.sf.hajdbc.dialect.StandardDialect#createForeignKeyConstraintFormat()
	 */
	@Override
	protected String createForeignKeyConstraintFormat()
	{
		return "ALTER TABLE {1} ADD CONSTRAINT {0} FOREIGN KEY ({2}) REFERENCES {3} ({4}) ON DELETE {5,choice,0#CASCADE|1#RESTRICT|2#SET NULL|3#NO ACTION|4#SET DEFAULT} ON UPDATE {6,choice,0#CASCADE|1#RESTRICT|2#SET NULL|3#NO ACTION|4#SET DEFAULT}";
	}

	/**
	 * @see net.sf.hajdbc.dialect.StandardDialect#currentDatePattern()
	 */
	@Override
	protected String currentDatePattern()
	{
		return "(?<=\\W)CURRENT_DATE(?:\\s*\\(\\s*\\))?(?=\\W)|(?<=\\W)CURDATE\\s*\\(\\s*\\)|(?<=\\W)SYSDATE(?=\\W)|(?<=\\W)TODAY(?=\\W)";
	}

	/**
	 * @see net.sf.hajdbc.dialect.StandardDialect#currentTimePattern()
	 */
	@Override
	protected String currentTimePattern()
	{
		return "(?<=\\W)CURRENT_TIME(?:\\s*\\(\\s*\\))?(?=\\W)|(?<=\\W)CURTIME\\s*\\(\\s*\\)";
	}

	/**
	 * @see net.sf.hajdbc.dialect.StandardDialect#currentTimestampPattern()
	 */
	@Override
	protected String currentTimestampPattern()
	{
		return "(?<=\\W)CURRENT_TIMESTAMP(?:\\s*\\(\\s*\\d*\\s*\\))?(?=\\W)|(?<=\\W)NOW\\s*\\(\\s*\\d*\\s*\\)";
	}

	/**
	 * @see net.sf.hajdbc.dialect.StandardDialect#randomPattern()
	 */
	@Override
	protected String randomPattern()
	{
		return "(?<=\\W)RAND\\s*\\(\\s*\\d*\\s*\\)";
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.dialect.StandardDialect#getDefaultSchemas(java.sql.DatabaseMetaData)
	 */
	@Override
	public List<String> getDefaultSchemas(DatabaseMetaData metaData)
	{
		return Collections.singletonList("PUBLIC");
	}
}
