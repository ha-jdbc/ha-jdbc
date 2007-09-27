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
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.MessageFormat;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import net.sf.hajdbc.ColumnProperties;
import net.sf.hajdbc.Dialect;
import net.sf.hajdbc.ForeignKeyConstraint;
import net.sf.hajdbc.QualifiedName;
import net.sf.hajdbc.SequenceProperties;
import net.sf.hajdbc.TableProperties;
import net.sf.hajdbc.UniqueConstraint;
import net.sf.hajdbc.util.Strings;

/**
 * @author  Paul Ferraro
 * @since   1.1
 */
@SuppressWarnings("nls")
public class StandardDialect implements Dialect
{
	private Pattern selectForUpdatePattern = this.compile(this.selectForUpdatePattern());
	private Pattern insertIntoTablePattern = this.compile(this.insertIntoTablePattern());
	private Pattern sequencePattern = this.compile(this.sequencePattern());
	private Pattern currentTimestampPattern = this.compile(this.currentTimestampPattern());
	private Pattern currentDatePattern = this.compile(this.currentDatePattern());
	private Pattern currentTimePattern = this.compile(this.currentTimePattern());
	private Pattern randomPattern = this.compile(this.randomPattern());
	
	private Pattern compile(String pattern)
	{
		return Pattern.compile(pattern, Pattern.CASE_INSENSITIVE);
	}
	
	protected String selectForUpdatePattern()
	{
		return "SELECT\\s+.+\\s+FOR\\s+UPDATE";
	}

	protected String insertIntoTablePattern()
	{
		return "INSERT\\s+(?:INTO\\s+)?'?([^'\\s\\(]+)";
	}

	protected String sequencePattern()
	{
		return "NEXT\\s+VALUE\\s+FOR\\s+'?([^',\\s\\(\\)]+)";
	}
	
	protected String currentDatePattern()
	{
		return "(?<=\\W)CURRENT_DATE(?=\\W)";
	}
	
	protected String currentTimePattern()
	{
		return "(?<=\\W)CURRENT_TIME(?:\\s*\\(\\s*\\d+\\s*\\))?(?=\\W)|(?<=\\W)LOCALTIME(?:\\s*\\(\\s*\\d+\\s*\\))?(?=\\W)";
	}

	protected String currentTimestampPattern()
	{
		return "(?<=\\W)CURRENT_TIMESTAMP(?:\\s*\\(\\s*\\d+\\s*\\))?(?=\\W)|(?<=\\W)LOCALTIMESTAMP(?:\\s*\\(\\s*\\d+\\s*\\))?(?=\\W)";
	}
	
	protected String randomPattern()
	{
		return "(?<=\\W)RAND\\s*\\(\\s*\\)";
	}

	/**
	 * @see net.sf.hajdbc.Dialect#getSimpleSQL()
	 */
	@Override
	public String getSimpleSQL()
	{
		return this.executeFunctionSQL(this.currentTimestampFunction());
	}

	protected String executeFunctionFormat()
	{
		StringBuilder builder = new StringBuilder("SELECT {0}");
		
		String dummyTable = this.dummyTable();
		
		if (dummyTable != null)
		{
			builder.append(" FROM ").append(dummyTable);
		}
		
		return builder.toString();
	}
	
	protected String executeFunctionSQL(String function)
	{
		return MessageFormat.format(this.executeFunctionFormat(), function);
	}
	
	protected String currentTimestampFunction()
	{
		return "CURRENT_TIMESTAMP";
	}
	
	protected String dummyTable()
	{
		return null;
	}
	
	/**
	 * @see net.sf.hajdbc.Dialect#getLockTableSQL(net.sf.hajdbc.TableProperties)
	 */
	@Override
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
				builder.append(Strings.PADDED_COMMA);
			}
		}
		
		return builder.toString();
	}

	/**
	 * @see net.sf.hajdbc.Dialect#getTruncateTableSQL(net.sf.hajdbc.TableProperties)
	 */
	@Override
	public String getTruncateTableSQL(TableProperties properties)
	{
		return MessageFormat.format(this.truncateTableFormat(), properties.getName());
	}
	
	protected String truncateTableFormat()
	{
		return "DELETE FROM {0}";
	}

	/**
	 * @see net.sf.hajdbc.Dialect#getCreateForeignKeyConstraintSQL(net.sf.hajdbc.ForeignKeyConstraint)
	 */
	@Override
	public String getCreateForeignKeyConstraintSQL(ForeignKeyConstraint key)
	{
		return MessageFormat.format(this.createForeignKeyConstraintFormat(), key.getName(), key.getTable(), Strings.join(key.getColumnList(), Strings.PADDED_COMMA), key.getForeignTable(), Strings.join(key.getForeignColumnList(), Strings.PADDED_COMMA), key.getDeleteRule(), key.getUpdateRule(), key.getDeferrability());
	}
	
	protected String createForeignKeyConstraintFormat()
	{
		return "ALTER TABLE {1} ADD CONSTRAINT {0} FOREIGN KEY ({2}) REFERENCES {3} ({4}) ON DELETE {5,choice,0#CASCADE|1#RESTRICT|2#SET NULL|3#NO ACTION|4#SET DEFAULT} ON UPDATE {6,choice,0#CASCADE|1#RESTRICT|2#SET NULL|3#NO ACTION|4#SET DEFAULT} {7,choice,5#DEFERRABLE INITIALLY DEFERRED|6#DEFERRABLE INITIALLY IMMEDIATE|7#NOT DEFERRABLE}";
	}
	
	/**
	 * @see net.sf.hajdbc.Dialect#getDropForeignKeyConstraintSQL(net.sf.hajdbc.ForeignKeyConstraint)
	 */
	@Override
	public String getDropForeignKeyConstraintSQL(ForeignKeyConstraint key)
	{
		return MessageFormat.format(this.dropForeignKeyConstraintFormat(), key.getName(), key.getTable());
	}
	
	protected String dropForeignKeyConstraintFormat()
	{
		return this.dropConstraintFormat();
	}
	
	protected String dropConstraintFormat()
	{
		return "ALTER TABLE {1} DROP CONSTRAINT {0}";
	}

	/**
	 * @see net.sf.hajdbc.Dialect#getCreateUniqueConstraintSQL(net.sf.hajdbc.UniqueConstraint)
	 */
	@Override
	public String getCreateUniqueConstraintSQL(UniqueConstraint constraint)
	{
		return MessageFormat.format(this.createUniqueConstraintFormat(), constraint.getName(), constraint.getTable(), Strings.join(constraint.getColumnList(), Strings.PADDED_COMMA));
	}
	
	protected String createUniqueConstraintFormat()
	{
		return "ALTER TABLE {1} ADD CONSTRAINT {0} UNIQUE ({2})";
	}

	/**
	 * @see net.sf.hajdbc.Dialect#getDropUniqueConstraintSQL(net.sf.hajdbc.UniqueConstraint)
	 */
	@Override
	public String getDropUniqueConstraintSQL(UniqueConstraint constraint)
	{
		return MessageFormat.format(this.dropUniqueConstraintFormat(), constraint.getName(), constraint.getTable());
	}
	
	protected String dropUniqueConstraintFormat()
	{
		return this.dropConstraintFormat();
	}

	/**
	 * @see net.sf.hajdbc.Dialect#isIdentity(net.sf.hajdbc.ColumnProperties)
	 */
	@Override
	public boolean isIdentity(ColumnProperties properties)
	{
		String remarks = properties.getRemarks();
		
		return (remarks != null) && remarks.contains("GENERATED BY DEFAULT AS IDENTITY");
	}

	/**
	 * @see net.sf.hajdbc.Dialect#isSelectForUpdate(java.lang.String)
	 */
	@Override
	public boolean isSelectForUpdate(String sql)
	{
		return this.selectForUpdatePattern.matcher(sql).find();
	}
	
	/**
	 * @see net.sf.hajdbc.Dialect#parseInsertTable(java.lang.String)
	 */
	@Override
	public String parseInsertTable(String sql)
	{
		return this.parse(this.insertIntoTablePattern, sql);
	}

	/**
	 * @see net.sf.hajdbc.Dialect#getDefaultSchemas(java.sql.DatabaseMetaData)
	 */
	@Override
	public List<String> getDefaultSchemas(DatabaseMetaData metaData) throws SQLException
	{
		return Collections.singletonList(metaData.getUserName());
	}

	protected String executeFunction(Connection connection, String function) throws SQLException
	{
		Statement statement = connection.createStatement();
		
		ResultSet resultSet = statement.executeQuery(this.executeFunctionSQL(function));
		
		resultSet.next();
		
		String value = resultSet.getString(1);
		
		resultSet.close();
		statement.close();
		
		return value;
	}

	protected List<String> executeQuery(Connection connection, String sql) throws SQLException
	{
		List<String> resultList = new LinkedList<String>();
		
		Statement statement = connection.createStatement();
		
		ResultSet resultSet = statement.executeQuery(sql);
		
		while (resultSet.next())
		{
			resultList.add(resultSet.getString(1));
		}
		
		resultSet.close();
		statement.close();
		
		return resultList;
	}

	/**
	 * @see net.sf.hajdbc.Dialect#parseSequence(java.lang.String)
	 */
	@Override
	public String parseSequence(String sql)
	{
		return this.parse(this.sequencePattern, sql);
	}

	/**
	 * @see net.sf.hajdbc.Dialect#getColumnType(net.sf.hajdbc.ColumnProperties)
	 */
	@Override
	public int getColumnType(ColumnProperties properties)
	{
		return properties.getType();
	}

	/**
	 * @see net.sf.hajdbc.Dialect#getSequences(java.sql.DatabaseMetaData)
	 */
	@Override
	public Collection<QualifiedName> getSequences(DatabaseMetaData metaData) throws SQLException
	{
		List<QualifiedName> sequenceList = new LinkedList<QualifiedName>();
		
		ResultSet resultSet = metaData.getTables(Strings.EMPTY, null, Strings.ANY, new String[] { this.sequenceTableType() });
		
		while (resultSet.next())
		{
			sequenceList.add(new QualifiedName(resultSet.getString("TABLE_SCHEM"), resultSet.getString("TABLE_NAME")));
		}
		
		resultSet.close();
		
		return sequenceList;
	}

	protected String sequenceTableType()
	{
		return "SEQUENCE";
	}

	/**
	 * @see net.sf.hajdbc.Dialect#getNextSequenceValueSQL(net.sf.hajdbc.SequenceProperties)
	 */
	@Override
	public String getNextSequenceValueSQL(SequenceProperties sequence)
	{
		return this.executeFunctionSQL(MessageFormat.format(this.nextSequenceValueFormat(), sequence.getName()));
	}
	
	protected String nextSequenceValueFormat()
	{
		return "NEXT VALUE FOR {0}";
	}
	
	/**
	 * @see net.sf.hajdbc.Dialect#getAlterSequenceSQL(net.sf.hajdbc.SequenceProperties, long)
	 */
	@Override
	public String getAlterSequenceSQL(SequenceProperties sequence, long value)
	{
		return MessageFormat.format(this.alterSequenceFormat(), sequence.getName(), value);
	}
	
	protected String alterSequenceFormat()
	{
		return "ALTER SEQUENCE {0} RESTART WITH {1}";
	}

	@Override
	public String getAlterIdentityColumnSQL(TableProperties table, ColumnProperties column, long value) throws SQLException
	{
		return MessageFormat.format(this.alterIdentityColumnFormat(), table.getName(), column.getName(), value);
	}

	protected String alterIdentityColumnFormat()
	{
		return "ALTER TABLE {0} ALTER COLUMN {1} RESTART WITH {2}";
	}
	
	/**
	 * @see net.sf.hajdbc.Dialect#getIdentifierPattern(java.sql.DatabaseMetaData)
	 */
	@Override
	public Pattern getIdentifierPattern(DatabaseMetaData metaData) throws SQLException
	{
		return Pattern.compile(MessageFormat.format("[\\w{0}]+", Pattern.quote(metaData.getExtraNameCharacters())));
	}

	protected String parse(Pattern pattern, String string)
	{
		Matcher matcher = pattern.matcher(string);
		
		return matcher.find() ? matcher.group(1) : null;
	}

	/**
	 * @see net.sf.hajdbc.Dialect#evaluateCurrentDate(java.lang.String, java.sql.Date)
	 */
	@Override
	public String evaluateCurrentDate(String sql, java.sql.Date date)
	{
		return this.evaluateTemporal(sql, this.currentDatePattern, date, this.dateLiteralFormat());
	}
	
	protected String dateLiteralFormat()
	{
		return "DATE ''{0}''";
	}

	/**
	 * @see net.sf.hajdbc.Dialect#evaluateCurrentTime(java.lang.String, java.sql.Time)
	 */
	@Override
	public String evaluateCurrentTime(String sql, java.sql.Time time)
	{
		return this.evaluateTemporal(sql, this.currentTimePattern, time, this.timeLiteralFormat());
	}
	
	protected String timeLiteralFormat()
	{
		return "TIME ''{0}''";
	}

	/**
	 * @see net.sf.hajdbc.Dialect#evaluateCurrentTimestamp(java.lang.String, java.sql.Timestamp)
	 */
	@Override
	public String evaluateCurrentTimestamp(String sql, java.sql.Timestamp timestamp)
	{
		return this.evaluateTemporal(sql, this.currentTimestampPattern, timestamp, this.timestampLiteralFormat());
	}
	
	protected String timestampLiteralFormat()
	{
		return "TIMESTAMP ''{0}''";
	}

	private String evaluateTemporal(String sql, Pattern pattern, java.util.Date date, String format)
	{
		return pattern.matcher(sql).replaceAll(MessageFormat.format(format, date.toString()));
	}

	/**
	 * @see net.sf.hajdbc.Dialect#evaluateRand(java.lang.String)
	 */
	@Override
	public String evaluateRand(String sql)
	{	
		StringBuffer buffer = new StringBuffer();
		Matcher matcher = this.randomPattern.matcher(sql);
		
		while (matcher.find())
		{
			matcher.appendReplacement(buffer, Double.toString(Math.random()));
		}
		
		return matcher.appendTail(buffer).toString();
	}
}
