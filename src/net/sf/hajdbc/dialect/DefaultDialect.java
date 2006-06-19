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

import java.sql.SQLException;
import java.text.MessageFormat;
import java.util.Collection;
import java.util.Iterator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import net.sf.hajdbc.DatabaseMetaDataCache;
import net.sf.hajdbc.Dialect;
import net.sf.hajdbc.ForeignKeyConstraint;
import net.sf.hajdbc.UniqueConstraint;
import net.sf.hajdbc.util.Strings;

/**
 * @author  Paul Ferraro
 * @since   1.1
 */
public class DefaultDialect implements Dialect
{
	private Pattern selectForUpdatePattern = Pattern.compile(this.selectForUpdatePattern(), Pattern.CASE_INSENSITIVE);
	private Pattern insertIntoTablePattern = Pattern.compile(this.insertIntoTablePattern(), Pattern.CASE_INSENSITIVE);
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
	public String getLockTableSQL(DatabaseMetaDataCache metaData, String schema, String table) throws SQLException
	{
		StringBuilder builder = new StringBuilder("UPDATE ").append(metaData.getQualifiedTableForDML(schema, table)).append(" SET ");
		
		UniqueConstraint primaryKey = metaData.getPrimaryKey(schema, table);
		
		Collection<String> columnList = (primaryKey != null) ? primaryKey.getColumnList() : metaData.getColumns(schema, table).keySet();

		Iterator<String> columns = columnList.iterator();
		
		while (columns.hasNext())
		{
			String column = columns.next();
			
			builder.append(column).append('=').append(column);
			
			if (columns.hasNext())
			{
				builder.append(',');
			}
		}
		
		return builder.toString();
	}

	/**
	 * @see net.sf.hajdbc.Dialect#getTruncateTableSQL(java.sql.DatabaseMetaData, java.lang.String, java.lang.String)
	 */
	public String getTruncateTableSQL(DatabaseMetaDataCache metaData, String schema, String table) throws SQLException
	{
		return MessageFormat.format(this.truncateTableFormat(), metaData.getQualifiedTableForDML(schema, table));
	}
	
	/**
	 * @see net.sf.hajdbc.Dialect#getCreateForeignKeyConstraintSQL(java.sql.DatabaseMetaData, java.lang.String, java.lang.String, java.lang.String, java.lang.String, java.lang.String, java.lang.String, java.lang.String)
	 */
	public String getCreateForeignKeyConstraintSQL(DatabaseMetaDataCache metaData, ForeignKeyConstraint key) throws SQLException
	{
		return MessageFormat.format(this.createForeignKeyFormat(), key.getName(), metaData.getQualifiedTableForDDL(key.getSchema(), key.getTable()), Strings.join(key.getColumnList(), ","), metaData.getQualifiedTableForDDL(key.getForeignSchema(), key.getForeignTable()), Strings.join(key.getForeignColumnList(), ","), key.getDeleteRule(), key.getUpdateRule(), key.getDeferrability());
	}
	
	/**
	 * @see net.sf.hajdbc.Dialect#getDropForeignKeyConstraintSQL(java.sql.DatabaseMetaData, java.lang.String, java.lang.String, java.lang.String)
	 */
	public String getDropForeignKeyConstraintSQL(DatabaseMetaDataCache metaData, ForeignKeyConstraint key) throws SQLException
	{
		return MessageFormat.format(this.dropConstraintFormat(), key.getName(), metaData.getQualifiedTableForDDL(key.getSchema(), key.getTable()));
	}
	
	/**
	 * @see net.sf.hajdbc.Dialect#getCreateUniqueConstraintSQL(java.sql.DatabaseMetaData, java.lang.String, java.lang.String, java.lang.String, java.util.List)
	 */
	public String getCreateUniqueConstraintSQL(DatabaseMetaDataCache metaData, UniqueConstraint constraint) throws SQLException
	{
		return MessageFormat.format(this.createUniqueKeyFormat(), constraint.getName(), metaData.getQualifiedTableForDDL(constraint.getSchema(), constraint.getTable()), Strings.join(constraint.getColumnList(), ","));
	}
	
	/**
	 * @see net.sf.hajdbc.Dialect#getDropUniqueConstraintSQL(java.sql.DatabaseMetaData, java.lang.String, java.lang.String, java.lang.String)
	 */
	public String getDropUniqueConstraintSQL(DatabaseMetaDataCache metaData, UniqueConstraint constraint) throws SQLException
	{
		return MessageFormat.format(this.dropConstraintFormat(), constraint.getName(), metaData.getQualifiedTableForDDL(constraint.getSchema(), constraint.getTable()));
	}

	/**
	 * @see net.sf.hajdbc.Dialect#getCreatePrimaryKeyConstraintSQL(net.sf.hajdbc.DatabaseMetaDataCache, net.sf.hajdbc.UniqueConstraint)
	 */
	public String getCreatePrimaryKeyConstraintSQL(DatabaseMetaDataCache metaData, UniqueConstraint constraint) throws SQLException
	{
		return MessageFormat.format(this.createPrimaryKeyFormat(), constraint.getName(), metaData.getQualifiedTableForDDL(constraint.getSchema(), constraint.getTable()), Strings.join(constraint.getColumnList(), ","));
	}

	/**
	 * @see net.sf.hajdbc.Dialect#getDropPrimaryKeyConstraintSQL(net.sf.hajdbc.DatabaseMetaDataCache, net.sf.hajdbc.UniqueConstraint)
	 */
	public String getDropPrimaryKeyConstraintSQL(DatabaseMetaDataCache metaData, UniqueConstraint constraint) throws SQLException
	{
		return MessageFormat.format(this.dropConstraintFormat(), constraint.getName(), metaData.getQualifiedTableForDDL(constraint.getSchema(), constraint.getTable()));
	}

	/**
	 * @see net.sf.hajdbc.Dialect#isSelectForUpdate(java.sql.DatabaseMetaData, java.lang.String)
	 */
	public boolean isSelectForUpdate(DatabaseMetaDataCache metaData, String sql) throws SQLException
	{
		return metaData.supportsSelectForUpdate() ? this.selectForUpdatePattern.matcher(sql).find() : false;
	}
	
	/**
	 * @see net.sf.hajdbc.Dialect#isInsertIntoTableWithAutoIncrementColumn(net.sf.hajdbc.DatabaseMetaDataCache, java.lang.String)
	 */
	public boolean isInsertIntoTableWithAutoIncrementColumn(DatabaseMetaDataCache metaData, String sql) throws SQLException
	{
		Matcher matcher = this.insertIntoTablePattern.matcher(sql);
		
		return matcher.find() ? metaData.containsAutoIncrementColumn(matcher.group(1)) : false;
	}
	
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
	public int getColumnType(DatabaseMetaDataCache metaData, String schema, String table, String column) throws SQLException
	{
		return metaData.getColumns(schema, table).get(column).getType();
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
	
	protected String insertIntoTablePattern()
	{
		return "INSERT\\s+(?:INTO\\s+)?(\\S+)";
	}
	
	protected String sequencePattern()
	{
		return "NEXT\\s+VALUE\\s+FOR\\s+(\\S+)";
	}
}
