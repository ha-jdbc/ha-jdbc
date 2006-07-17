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

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.text.MessageFormat;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.regex.Pattern;

import net.sf.hajdbc.Dialect;
import net.sf.hajdbc.ForeignKeyConstraint;
import net.sf.hajdbc.UniqueConstraint;

/**
 * @author  Paul Ferraro
 * @since   1.1
 */
public class DefaultDialect implements Dialect
{
	private Pattern selectForUpdatePattern = Pattern.compile(this.selectForUpdatePattern(), Pattern.CASE_INSENSITIVE);

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
	public String getLockTableSQL(DatabaseMetaData metaData, String schema, String table) throws SQLException
	{
		StringBuilder builder = new StringBuilder("UPDATE ").append(this.qualifyTable(metaData, schema, table)).append(" SET ");
		
		List<String> columnList = new LinkedList<String>();
		
		ResultSet resultSet = metaData.getPrimaryKeys(null, schema, table);
		
		while (resultSet.next())
		{
			columnList.add(resultSet.getString("COLUMN_NAME"));
		}
		
		resultSet.close();

		// If table contains no primary keys, use all columns in table instead
		if (columnList.isEmpty())
		{
			resultSet = metaData.getColumns(null, schema, table, "%");
			
			while (resultSet.next())
			{
				columnList.add(resultSet.getString("COLUMN_NAME"));
			}
			
			resultSet.close();
		}
		
		Iterator<String> columns = columnList.iterator();
		
		while (columns.hasNext())
		{
			String column = this.quote(metaData, columns.next());
			
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
	public String getTruncateTableSQL(DatabaseMetaData metaData, String schema, String table) throws SQLException
	{
		return MessageFormat.format(this.truncateTablePattern(), this.qualifyTable(metaData, schema, table));
	}
	
	/**
	 * @see net.sf.hajdbc.Dialect#qualifyTable(java.sql.DatabaseMetaData, java.lang.String, java.lang.String)
	 */
	public String qualifyTable(DatabaseMetaData metaData, String schema, String table) throws SQLException
	{
		StringBuilder builder = new StringBuilder();
		
		if ((schema != null) && metaData.supportsSchemasInDataManipulation())
		{
			builder.append(this.quote(metaData, schema)).append('.');
		}
		
		return builder.append(this.quote(metaData, table)).toString();
	}
	
	/**
	 * @see net.sf.hajdbc.Dialect#quote(java.sql.DatabaseMetaData, java.lang.String)
	 */
	public String quote(DatabaseMetaData metaData, String identifier) throws SQLException
	{
		String quote = metaData.getIdentifierQuoteString();
		
		return identifier.startsWith(quote) ? identifier : quote + identifier + quote;
	}
	
	/**
	 * @see net.sf.hajdbc.Dialect#getCreateForeignKeyConstraintSQL(java.sql.DatabaseMetaData, net.sf.hajdbc.ForeignKeyConstraint)
	 */
	public String getCreateForeignKeyConstraintSQL(DatabaseMetaData metaData, ForeignKeyConstraint key) throws SQLException
	{
		return MessageFormat.format(this.createForeignKeyPattern(), this.quote(metaData, key.getName()), this.qualifyTable(metaData, key.getSchema(), key.getTable()), this.joinColumns(metaData, key.getColumnList()), this.qualifyTable(metaData, key.getForeignSchema(), key.getForeignTable()), this.joinColumns(metaData, key.getForeignColumnList()), key.getDeleteRule(), key.getUpdateRule(), key.getDeferrability());
	}
	
	/**
	 * @see net.sf.hajdbc.Dialect#getDropForeignKeyConstraintSQL(java.sql.DatabaseMetaData, net.sf.hajdbc.ForeignKeyConstraint)
	 */
	public String getDropForeignKeyConstraintSQL(DatabaseMetaData metaData, ForeignKeyConstraint key) throws SQLException
	{
		return MessageFormat.format(this.dropConstraintPattern(), this.quote(metaData, key.getName()), this.qualifyTable(metaData, key.getSchema(), key.getTable()));
	}
	
	/**
	 * @see net.sf.hajdbc.Dialect#getCreateUniqueConstraintSQL(java.sql.DatabaseMetaData, net.sf.hajdbc.UniqueConstraint)
	 */
	public String getCreateUniqueConstraintSQL(DatabaseMetaData metaData, UniqueConstraint constraint) throws SQLException
	{
		return MessageFormat.format(this.createUniqueKeyPattern(), this.quote(metaData, constraint.getName()), this.qualifyTable(metaData, constraint.getSchema(), constraint.getTable()), this.joinColumns(metaData, constraint.getColumnList()));
	}
	
	/**
	 * @see net.sf.hajdbc.Dialect#getDropUniqueConstraintSQL(java.sql.DatabaseMetaData, net.sf.hajdbc.UniqueConstraint)
	 */
	public String getDropUniqueConstraintSQL(DatabaseMetaData metaData, UniqueConstraint constraint) throws SQLException
	{
		return MessageFormat.format(this.dropConstraintPattern(), this.quote(metaData, constraint.getName()), this.qualifyTable(metaData, constraint.getSchema(), constraint.getTable()));
	}

	/**
	 * @see net.sf.hajdbc.Dialect#isSelectForUpdate(java.sql.DatabaseMetaData, java.lang.String)
	 */
	public boolean isSelectForUpdate(DatabaseMetaData metaData, String sql) throws SQLException
	{
		return metaData.supportsSelectForUpdate() ? this.selectForUpdatePattern.matcher(sql).find() : false;
	}
	
	/**
	 * @see net.sf.hajdbc.Dialect#getColumnType(java.sql.ResultSetMetaData, int)
	 */
	public int getColumnType(ResultSetMetaData metaData, int column) throws SQLException
	{
		return metaData.getColumnType(column);
	}

	protected String joinColumns(DatabaseMetaData metaData, List<String> columnList) throws SQLException
	{
		StringBuilder builder = new StringBuilder();

		Iterator<String> columns = columnList.iterator();
		
		while (columns.hasNext())
		{
			builder.append(this.quote(metaData, columns.next()));
			
			if (columns.hasNext())
			{
				builder.append(',');
			}
		}
		
		return builder.toString();
	}
	
	/**
	 * SQL-92 compatible DELETE statement
	 * @return a <code>java.text.MessageFormat</code> pattern.
	 */
	protected String truncateTablePattern()
	{
		return "DELETE FROM {0}";
	}
	
	/**
	 * Returns a SQL-92 compatible create foreign key constraint statement pattern.
	 * @return a <code>java.text.MessageFormat</code> pattern.
	 */
	protected String createForeignKeyPattern()
	{
		return "ALTER TABLE {1} ADD CONSTRAINT {0} FOREIGN KEY ({2}) REFERENCES {3} ({4}) ON DELETE {5,choice,0#CASCADE|1#RESTRICT|2#SET NULL|3#NO ACTION|4#SET DEFAULT} ON UPDATE {6,choice,0#CASCADE|1#RESTRICT|2#SET NULL|3#NO ACTION|4#SET DEFAULT} {7,choice,5#DEFERRABLE INITIALLY DEFERRED|6#DEFERRABLE INITIALLY IMMEDIATE|7#NOT DEFERRABLE}";
	}
	
	/**
	 * Returns a SQL-92 compatible create unique constraint statement pattern.
	 * @return a <code>java.text.MessageFormat</code> pattern.
	 */
	protected String createUniqueKeyPattern()
	{
		return "ALTER TABLE {1} ADD CONSTRAINT {0} UNIQUE ({2})";
	}
	
	/**
	 * Returns a SQL-92 compatible DROP CONSTRAINT statement pattern.
	 * @return a <code>java.text.MessageFormat</code> pattern.
	 */
	protected String dropConstraintPattern()
	{
		return "ALTER TABLE {1} DROP CONSTRAINT {0}";
	}
	
	/**
	 * Returns a SQL-92 compatible SELECT...FOR UPDATE statement pattern.
	 * @return a regular expression pattern.
	 */
	protected String selectForUpdatePattern()
	{
		return "SELECT\\s+.+\\s+FOR\\s+UPDATE";
	}
}
