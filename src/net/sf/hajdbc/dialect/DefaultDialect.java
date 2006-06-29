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
import java.sql.SQLException;
import java.text.MessageFormat;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import net.sf.hajdbc.ColumnProperties;
import net.sf.hajdbc.Dialect;
import net.sf.hajdbc.ForeignKeyConstraint;
import net.sf.hajdbc.TableProperties;
import net.sf.hajdbc.UniqueConstraint;
import net.sf.hajdbc.util.Strings;

/**
 * @author  Paul Ferraro
 * @since   1.1
 */
public class DefaultDialect implements Dialect
{
	private Pattern selectForUpdatePattern = Pattern.compile(this.selectForUpdatePattern(), Pattern.CASE_INSENSITIVE);
//	private Pattern insertIntoTablePattern = Pattern.compile(this.insertIntoTablePattern(), Pattern.CASE_INSENSITIVE);
	private Pattern sequencePattern = Pattern.compile(this.sequencePattern(), Pattern.CASE_INSENSITIVE);
	
	/**
	 * @see net.sf.hajdbc.Dialect#getSimpleSQL()
	 */
	public String getSimpleSQL()
	{
		return "SELECT 1";
	}

	/**
	 * @see net.sf.hajdbc.Dialect#getLockTableSQL(java.sql.DatabaseMetaData, java.lang.String, java.lang.String)
	 */
	public String getLockTableSQL(TableProperties properties) throws SQLException
	{
		StringBuilder builder = new StringBuilder("UPDATE ").append(properties.getName()).append(" SET ");
		
		UniqueConstraint primaryKey = properties.getPrimaryKey();
		
		Collection<String> columnList = (primaryKey != null) ? primaryKey.getColumnList() : properties.getColumns();

		Iterator<String> columns = columnList.iterator();
		
		while (columns.hasNext())
		{
			String column = columns.next();
			
			builder.append(column).append(" = ").append(column);
			
			if (columns.hasNext())
			{
				builder.append(", ");
			}
		}
		
		return builder.toString();
	}

	/**
	 * @see net.sf.hajdbc.Dialect#getTruncateTableSQL(java.sql.DatabaseMetaData, java.lang.String, java.lang.String)
	 */
	public String getTruncateTableSQL(TableProperties properties) throws SQLException
	{
		return MessageFormat.format(this.truncateTableFormat(), properties.getName());
	}
	
	/**
	 * @see net.sf.hajdbc.Dialect#getCreateForeignKeyConstraintSQL(java.sql.DatabaseMetaData, java.lang.String, java.lang.String, java.lang.String, java.lang.String, java.lang.String, java.lang.String, java.lang.String)
	 */
	public String getCreateForeignKeyConstraintSQL(ForeignKeyConstraint key)
	{
		return MessageFormat.format(this.createForeignKeyFormat(), key.getName(), key.getTable(), Strings.join(key.getColumnList(), ", "), key.getForeignTable(), Strings.join(key.getForeignColumnList(), ", "), key.getDeleteRule(), key.getUpdateRule(), key.getDeferrability());
	}
	
	/**
	 * @see net.sf.hajdbc.Dialect#getDropForeignKeyConstraintSQL(java.sql.DatabaseMetaData, java.lang.String, java.lang.String, java.lang.String)
	 */
	public String getDropForeignKeyConstraintSQL(ForeignKeyConstraint key)
	{
		return MessageFormat.format(this.dropConstraintFormat(), key.getName(), key.getTable());
	}
	
	/**
	 * @see net.sf.hajdbc.Dialect#getCreateUniqueConstraintSQL(java.sql.DatabaseMetaData, java.lang.String, java.lang.String, java.lang.String, java.util.List)
	 */
	public String getCreateUniqueConstraintSQL(UniqueConstraint constraint)
	{
		return MessageFormat.format(this.createUniqueKeyFormat(), constraint.getName(), constraint.getTable(), Strings.join(constraint.getColumnList(), ", "));
	}
	
	/**
	 * @see net.sf.hajdbc.Dialect#getDropUniqueConstraintSQL(java.sql.DatabaseMetaData, java.lang.String, java.lang.String, java.lang.String)
	 */
	public String getDropUniqueConstraintSQL(UniqueConstraint constraint)
	{
		return MessageFormat.format(this.dropConstraintFormat(), constraint.getName(), constraint.getTable());
	}

	/**
	 * @see net.sf.hajdbc.Dialect#getCreatePrimaryKeyConstraintSQL(net.sf.hajdbc.DatabaseMetaDataCache, net.sf.hajdbc.UniqueConstraint)
	 */
	public String getCreatePrimaryKeyConstraintSQL(UniqueConstraint constraint)
	{
		return MessageFormat.format(this.createPrimaryKeyFormat(), constraint.getName(), constraint.getTable(), Strings.join(constraint.getColumnList(), ", "));
	}

	/**
	 * @see net.sf.hajdbc.Dialect#getDropPrimaryKeyConstraintSQL(net.sf.hajdbc.DatabaseMetaDataCache, net.sf.hajdbc.UniqueConstraint)
	 */
	public String getDropPrimaryKeyConstraintSQL(UniqueConstraint constraint)
	{
		return MessageFormat.format(this.dropConstraintFormat(), constraint.getName(), constraint.getTable());
	}

	/**
	 * @see net.sf.hajdbc.Dialect#isSelectForUpdate(java.sql.DatabaseMetaData, java.lang.String)
	 */
	public boolean isSelectForUpdate(String sql)
	{
		return this.selectForUpdatePattern.matcher(sql).find();
	}
	
	/**
	 * @see net.sf.hajdbc.Dialect#isInsertIntoTableWithAutoIncrementColumn(net.sf.hajdbc.DatabaseMetaDataCache, java.lang.String)
	 */
/*	public String parseInsertTable(DatabaseMetaDataCache metaData, String sql) throws SQLException
	{
		Matcher matcher = this.insertIntoTablePattern.matcher(sql);
		
		return matcher.find() ? matcher.group(1) : null;
	}
*/	
	/**
	 * @see net.sf.hajdbc.Dialect#parseSequence(java.lang.String)
	 */
	public String parseSequence(String sql)
	{
		Matcher matcher = this.sequencePattern.matcher(sql);
		
		return matcher.find() ? matcher.group(1) : null;
	}

	/**
	 * @see net.sf.hajdbc.Dialect#getColumnType(net.sf.hajdbc.cache.ColumnProperties)
	 */
	public int getColumnType(ColumnProperties properties)
	{
		return properties.getType();
	}

	/**
	 * JDBC API does not acknowledge the existence of sequences.  Each dialect 
	 * @see net.sf.hajdbc.Dialect#getSequences()
	 */
	@SuppressWarnings("unused")
	public Map<String, Long> getSequences(Connection connection) throws SQLException
	{
		return Collections.emptyMap();
	}

	/**
	 * @see net.sf.hajdbc.Dialect#getAlterSequenceSQL(java.lang.String, long)
	 */
	public String getAlterSequenceSQL(String sequence, long value)
	{
		return MessageFormat.format(this.alterSequenceFormat(), sequence, value);
	}

	protected String truncateTableFormat()
	{
		return "DELETE FROM {0}";
	}
	
	protected String createForeignKeyFormat()
	{
		return "ALTER TABLE {1} ADD CONSTRAINT {0} FOREIGN KEY ({2}) REFERENCES {3} ({4}) ON DELETE {5,choice,0#CASCADE|1#RESTRICT|2#SET NULL|3#NO ACTION|4#SET DEFAULT} ON UPDATE {6,choice,0#CASCADE|1#RESTRICT|2#SET NULL|3#NO ACTION|4#SET DEFAULT} {7,choice,5#DEFERRABLE INITIALLY DEFERRED|6#DEFERRABLE INITIALLY IMMEDIATE|7#NOT DEFERRABLE}";
	}
	
	protected String createUniqueKeyFormat()
	{
		return "ALTER TABLE {1} ADD CONSTRAINT {0} UNIQUE ({2})";
	}
	
	protected String createPrimaryKeyFormat()
	{
		return "ALTER TABLE {1} ADD CONSTRAINT {0} PRIMARY KEY ({2})";
	}
	
	protected String dropConstraintFormat()
	{
		return "ALTER TABLE {1} DROP CONSTRAINT {0}";
	}
	
	protected String selectForUpdatePattern()
	{
		return "SELECT\\s+.+\\s+FOR\\s+UPDATE";
	}
/*	
	protected String insertIntoTablePattern()
	{
		return "INSERT\\s+(?:INTO\\s+)?(\\S+)";
	}
*/	
	protected String sequencePattern()
	{
		return "NEXT\\s+VALUE\\s+FOR\\s+(\\S+)";
	}
	
	protected String alterSequenceFormat()
	{
		return "ALTER SEQUENCE {0} RESTART WITH {1}";
	}
}
