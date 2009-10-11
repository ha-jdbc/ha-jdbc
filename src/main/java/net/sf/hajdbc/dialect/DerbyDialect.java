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
import java.sql.SQLException;
import java.util.Collection;
import java.util.Collections;

import net.sf.hajdbc.cache.QualifiedName;

/**
 * Dialect for <a href="http://db.apache.org/derby">Apache Derby</a>.
 * 
 * @author  Paul Ferraro
 * @since   1.1
 */
@SuppressWarnings("nls")
public class DerbyDialect extends StandardDialect
{
	/**
	 * @see net.sf.hajdbc.dialect.StandardDialect#executeFunctionFormat()
	 */
	@Override
	protected String executeFunctionFormat()
	{
		return "VALUES {0}";
	}

	/**
	 * @see net.sf.hajdbc.dialect.StandardDialect#parseSequence(java.lang.String)
	 */
	@Override
	public String parseSequence(String sql)
	{
		return null;
	}
	
	/**
	 * @see net.sf.hajdbc.dialect.StandardDialect#getSequences(java.sql.DatabaseMetaData)
	 */
	@Override
	public Collection<QualifiedName> getSequences(DatabaseMetaData metaData) throws SQLException
	{
		return Collections.emptyList();
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
		return super.currentDatePattern() + "|(?<=\\W)CURRENT\\s+DATE(?=\\W)";
	}

	/**
	 * @see net.sf.hajdbc.dialect.StandardDialect#currentTimePattern()
	 */
	@Override
	protected String currentTimePattern()
	{
		return super.currentTimePattern() + "|(?<=\\W)CURRENT\\s+TIME(?=\\W)";
	}

	/**
	 * @see net.sf.hajdbc.dialect.StandardDialect#currentTimestampPattern()
	 */
	@Override
	protected String currentTimestampPattern()
	{
		return super.currentTimestampPattern() + "|(?<=\\W)CURRENT\\s+TIMESTAMP(?=\\W)";
	}

	/**
	 * @see net.sf.hajdbc.dialect.StandardDialect#dateLiteralFormat()
	 */
	@Override
	protected String dateLiteralFormat()
	{
		return "DATE(''{0}'')";
	}

	/**
	 * @see net.sf.hajdbc.dialect.StandardDialect#timeLiteralFormat()
	 */
	@Override
	protected String timeLiteralFormat()
	{
		return "TIME(''{0}'')";
	}

	/**
	 * @see net.sf.hajdbc.dialect.StandardDialect#timestampLiteralFormat()
	 */
	@Override
	protected String timestampLiteralFormat()
	{
		return "TIMESTAMP(''{0}'')";
	}
}
