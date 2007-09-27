/*
 * Copyright (c) 2004-2007, Identity Theft 911, LLC.  All rights reserved.
 */
package net.sf.hajdbc.dialect;

import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.text.MessageFormat;
import java.util.Collection;
import java.util.Collections;

import net.sf.hajdbc.ColumnProperties;
import net.sf.hajdbc.QualifiedName;
import net.sf.hajdbc.TableProperties;

/**
 * Dialect for Sybase (commercial).
 * @author Paul Ferraro
 */
@SuppressWarnings("nls")
public class SybaseDialect extends StandardDialect
{
	/**
	 * @see net.sf.hajdbc.dialect.StandardDialect#getLockTableSQL(net.sf.hajdbc.TableProperties)
	 */
	@Override
	public String getLockTableSQL(TableProperties properties) throws SQLException
	{
		return MessageFormat.format("LOCK TABLE {0} IN SHARE MODE", properties.getName());
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
	 * Deferrability clause is not supported.
	 * @see net.sf.hajdbc.dialect.StandardDialect#createForeignKeyConstraintFormat()
	 */
	@Override
	protected String createForeignKeyConstraintFormat()
	{
		return "ALTER TABLE {1} ADD CONSTRAINT {0} FOREIGN KEY ({2}) REFERENCES {3} ({4}) ON DELETE {5,choice,0#CASCADE|1#RESTRICT|2#SET NULL|3#NO ACTION|4#SET DEFAULT} ON UPDATE {6,choice,0#CASCADE|1#RESTRICT|2#SET NULL|3#NO ACTION|4#SET DEFAULT}";
	}

	/**
	 * @see net.sf.hajdbc.dialect.StandardDialect#isIdentity(net.sf.hajdbc.ColumnProperties)
	 */
	@Override
	public boolean isIdentity(ColumnProperties properties)
	{
		String defaultValue = properties.getDefaultValue();
		
		return (defaultValue != null) && (defaultValue.equalsIgnoreCase("AUTOINCREMENT") || defaultValue.equalsIgnoreCase("IDENTITY"));
	}

	/**
	 * Sybase does not support sequences.
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
	 * @see net.sf.hajdbc.dialect.StandardDialect#currentDatePattern()
	 */
	@Override
	protected String currentDatePattern()
	{
		return "(?<=\\W)CURRENT\\s+DATE(?=\\W)|(?<=\\W)TODAY\\s*\\(\\s*\\*\\s*\\)";
	}

	/**
	 * @see net.sf.hajdbc.dialect.StandardDialect#currentTimePattern()
	 */
	@Override
	protected String currentTimePattern()
	{
		return "(?<=\\W)CURRENT\\s+TIME(?=\\W)";
	}

	/**
	 * @see net.sf.hajdbc.dialect.StandardDialect#currentTimestampPattern()
	 */
	@Override
	protected String currentTimestampPattern()
	{
		return "(?<=\\W)CURRENT\\s+TIMESTAMP(?=\\W)|(?<=\\W)GETDATE\\s*\\(\\s*\\)|(?<=\\W)NOW\\s*\\(\\s*\\*\\s*\\)";
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
	
	/**
	 * @see net.sf.hajdbc.dialect.StandardDialect#randomPattern()
	 */
	@Override
	protected String randomPattern()
	{
		return "(?<=\\W)RAND\\s*\\(\\s*\\d*\\s*\\)";
	}	
}
