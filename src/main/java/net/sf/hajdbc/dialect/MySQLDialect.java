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
import java.util.Collections;
import java.util.List;
import java.util.Map;

import net.sf.hajdbc.cache.QualifiedName;

/**
 * Dialect for <a href="http://www.mysql.com/products/database/mysql/">MySQL</a>
 * @author Paul Ferraro
 */
@SuppressWarnings("nls")
public class MySQLDialect extends StandardDialect
{
	private static final long serialVersionUID = 1108382737506332406L;

	/**
	 * @see net.sf.hajdbc.dialect.StandardDialect#getDefaultSchemas(java.sql.DatabaseMetaData)
	 */
	@Override
	public List<String> getDefaultSchemas(DatabaseMetaData metaData) throws SQLException
	{
		return Collections.singletonList(this.executeFunction(metaData.getConnection(), "DATABASE()"));
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
	public Map<QualifiedName, Integer> getSequences(DatabaseMetaData metaData) throws SQLException
	{
		return Collections.emptyMap();
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
	 * @see net.sf.hajdbc.dialect.StandardDialect#createUniqueConstraintFormat()
	 */
	@Override
	protected String createUniqueConstraintFormat()
	{
		return "ALTER TABLE {1} ADD UNIQUE {0} ({2})";
	}

	/**
	 * @see net.sf.hajdbc.dialect.StandardDialect#dropForeignKeyConstraintFormat()
	 */
	@Override
	protected String dropForeignKeyConstraintFormat()
	{
		return "ALTER TABLE {1} DROP FOREIGN KEY {0}";
	}

	/**
	 * @see net.sf.hajdbc.dialect.StandardDialect#dropUniqueConstraintFormat()
	 */
	@Override
	protected String dropUniqueConstraintFormat()
	{
		return "ALTER TABLE {1} DROP INDEX {0}";
	}

	/**
	 * @see net.sf.hajdbc.dialect.StandardDialect#alterIdentityColumnFormat()
	 */
	@Override
	protected String alterIdentityColumnFormat()
	{
		return "ALTER TABLE {0} AUTO_INCREMENT = {2}";
	}

	/**
	 * @see net.sf.hajdbc.dialect.StandardDialect#currentDatePattern()
	 */
	@Override
	protected String currentDatePattern()
	{
		return super.currentDatePattern() + "|(?<=\\W)CURDATE\\s*\\(\\s*\\)";
	}

	/**
	 * @see net.sf.hajdbc.dialect.StandardDialect#currentTimePattern()
	 */
	@Override
	protected String currentTimePattern()
	{
		return super.currentTimePattern() + "|(?<=\\W)CURTIME\\s*\\(\\s*\\)";
	}

	/**
	 * @see net.sf.hajdbc.dialect.StandardDialect#currentTimestampPattern()
	 */
	@Override
	protected String currentTimestampPattern()
	{
		return super.currentTimestampPattern() + "|(?<=\\W)NOW\\s*\\(\\s*\\)|(?<=\\W)SYSDATE\\s*\\(\\s*\\)";
	}
	
	/**
	 * @see net.sf.hajdbc.dialect.StandardDialect#dateLiteralFormat()
	 */
	@Override
	protected String dateLiteralFormat()
	{
		return this.timestampLiteralFormat();
	}

	/**
	 * @see net.sf.hajdbc.dialect.StandardDialect#timeLiteralFormat()
	 */
	@Override
	protected String timeLiteralFormat()
	{
		return this.timestampLiteralFormat();
	}

	/**
	 * @see net.sf.hajdbc.dialect.StandardDialect#timestampLiteralFormat()
	 */
	@Override
	protected String timestampLiteralFormat()
	{
		return "''{0}''";
	}
}
