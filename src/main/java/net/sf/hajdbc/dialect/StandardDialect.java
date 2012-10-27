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
package net.sf.hajdbc.dialect;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLNonTransientConnectionException;
import java.sql.Statement;
import java.text.MessageFormat;
import java.util.AbstractMap;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.transaction.xa.XAException;

import net.sf.hajdbc.ColumnProperties;
import net.sf.hajdbc.ColumnPropertiesFactory;
import net.sf.hajdbc.DumpRestoreSupport;
import net.sf.hajdbc.ForeignKeyConstraint;
import net.sf.hajdbc.ForeignKeyConstraintFactory;
import net.sf.hajdbc.IdentifierNormalizer;
import net.sf.hajdbc.IdentityColumnSupport;
import net.sf.hajdbc.QualifiedName;
import net.sf.hajdbc.QualifiedNameFactory;
import net.sf.hajdbc.SequenceProperties;
import net.sf.hajdbc.SequencePropertiesFactory;
import net.sf.hajdbc.SequenceSupport;
import net.sf.hajdbc.TableProperties;
import net.sf.hajdbc.TriggerEvent;
import net.sf.hajdbc.TriggerSupport;
import net.sf.hajdbc.TriggerTime;
import net.sf.hajdbc.UniqueConstraint;
import net.sf.hajdbc.UniqueConstraintFactory;
import net.sf.hajdbc.util.Resources;
import net.sf.hajdbc.util.Strings;

/**
 * @author  Paul Ferraro
 * @since   1.1
 */
public class StandardDialect implements Dialect, SequenceSupport, IdentityColumnSupport, TriggerSupport
{
	private final Pattern selectForUpdatePattern = this.compile(this.selectForUpdatePattern());
	private final Pattern insertIntoTablePattern = this.compile(this.insertIntoTablePattern());
	private final Pattern sequencePattern = this.compile(this.sequencePattern());
	private final Pattern currentTimestampPattern = this.compile(this.currentTimestampPattern());
	private final Pattern currentDatePattern = this.compile(this.currentDatePattern());
	private final Pattern currentTimePattern = this.compile(this.currentTimePattern());
	private final Pattern randomPattern = this.compile(this.randomPattern());
	private final Pattern urlPattern = Pattern.compile(String.format("jdbc:%s:%s", this.vendorPattern(), this.locatorPattern()));
	
	protected String vendorPattern()
	{
		return "(?:^\\:)+";
	}

	protected String locatorPattern()
	{
		return "//(^\\:+):(\\d+)/(^\\?+)(?:\\?.*)?";
	}
	
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
	 * @see net.sf.hajdbc.dialect.Dialect#getSimpleSQL()
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
	 * @see net.sf.hajdbc.dialect.Dialect#getTruncateTableSQL(net.sf.hajdbc.TableProperties)
	 */
	@Override
	public String getTruncateTableSQL(TableProperties properties)
	{
		return MessageFormat.format(this.truncateTableFormat(), properties.getName().getDMLName());
	}
	
	protected String truncateTableFormat()
	{
		return "DELETE FROM {0}";
	}

	/**
	 * @see net.sf.hajdbc.dialect.Dialect#getCreateForeignKeyConstraintSQL(net.sf.hajdbc.ForeignKeyConstraint)
	 */
	@Override
	public String getCreateForeignKeyConstraintSQL(ForeignKeyConstraint key)
	{
		return MessageFormat.format(this.createForeignKeyConstraintFormat(), key.getName(), key.getTable().getDDLName(), Strings.join(key.getColumnList(), Strings.PADDED_COMMA), key.getForeignTable().getDDLName(), Strings.join(key.getForeignColumnList(), Strings.PADDED_COMMA), key.getDeleteRule(), key.getUpdateRule(), key.getDeferrability());
	}
	
	protected String createForeignKeyConstraintFormat()
	{
		return "ALTER TABLE {1} ADD CONSTRAINT {0} FOREIGN KEY ({2}) REFERENCES {3} ({4}) ON DELETE {5,choice,0#CASCADE|1#RESTRICT|2#SET NULL|3#NO ACTION|4#SET DEFAULT} ON UPDATE {6,choice,0#CASCADE|1#RESTRICT|2#SET NULL|3#NO ACTION|4#SET DEFAULT} {7,choice,5#DEFERRABLE INITIALLY DEFERRED|6#DEFERRABLE INITIALLY IMMEDIATE|7#NOT DEFERRABLE}";
	}
	
	/**
	 * @see net.sf.hajdbc.dialect.Dialect#getDropForeignKeyConstraintSQL(net.sf.hajdbc.ForeignKeyConstraint)
	 */
	@Override
	public String getDropForeignKeyConstraintSQL(ForeignKeyConstraint key)
	{
		return MessageFormat.format(this.dropForeignKeyConstraintFormat(), key.getName(), key.getTable().getDDLName());
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
	 * @see net.sf.hajdbc.dialect.Dialect#getCreateUniqueConstraintSQL(net.sf.hajdbc.UniqueConstraint)
	 */
	@Override
	public String getCreateUniqueConstraintSQL(UniqueConstraint constraint)
	{
		return MessageFormat.format(this.createUniqueConstraintFormat(), constraint.getName(), constraint.getTable().getDDLName(), Strings.join(constraint.getColumnList(), Strings.PADDED_COMMA));
	}
	
	protected String createUniqueConstraintFormat()
	{
		return "ALTER TABLE {1} ADD CONSTRAINT {0} UNIQUE ({2})";
	}

	/**
	 * @see net.sf.hajdbc.dialect.Dialect#getDropUniqueConstraintSQL(net.sf.hajdbc.UniqueConstraint)
	 */
	@Override
	public String getDropUniqueConstraintSQL(UniqueConstraint constraint)
	{
		return MessageFormat.format(this.dropUniqueConstraintFormat(), constraint.getName(), constraint.getTable().getDDLName());
	}
	
	protected String dropUniqueConstraintFormat()
	{
		return this.dropConstraintFormat();
	}

	/**
	 * @see net.sf.hajdbc.dialect.Dialect#isSelectForUpdate(java.lang.String)
	 */
	@Override
	public boolean isSelectForUpdate(String sql)
	{
		return this.selectForUpdatePattern.matcher(sql).find();
	}

	/**
	 * @see net.sf.hajdbc.dialect.Dialect#getDefaultSchemas(java.sql.DatabaseMetaData)
	 */
	@Override
	public List<String> getDefaultSchemas(DatabaseMetaData metaData) throws SQLException
	{
		return Collections.singletonList(metaData.getUserName());
	}

	protected String executeFunction(Connection connection, String function) throws SQLException
	{
		Statement statement = connection.createStatement();
		
		try
		{
			ResultSet resultSet = statement.executeQuery(this.executeFunctionSQL(function));
			
			resultSet.next();
			
			return resultSet.getString(1);
		}
		finally
		{
			Resources.close(statement);
		}
	}

	protected List<String> executeQuery(Connection connection, String sql) throws SQLException
	{
		Statement statement = connection.createStatement();
		
		try
		{
			ResultSet resultSet = statement.executeQuery(sql);
			
			List<String> resultList = new LinkedList<String>();
			
			while (resultSet.next())
			{
				resultList.add(resultSet.getString(1));
			}
			
			return resultList;
		}
		finally
		{
			Resources.close(statement);
		}
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.dialect.Dialect#getSequenceSupport()
	 */
	@Override
	public SequenceSupport getSequenceSupport()
	{
		return null;
	}

	/**
	 * @see net.sf.hajdbc.SequenceSupport#parseSequence(java.lang.String)
	 */
	@Override
	public String parseSequence(String sql)
	{
		return this.parse(this.sequencePattern, sql);
	}

	/**
	 * @see net.sf.hajdbc.dialect.Dialect#getColumnType(net.sf.hajdbc.ColumnProperties)
	 */
	@Override
	public int getColumnType(ColumnProperties properties)
	{
		return properties.getType();
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.SequenceSupport#getSequences(java.sql.DatabaseMetaData, net.sf.hajdbc.SequencePropertiesFactory)
	 */
	@Override
	public Collection<SequenceProperties> getSequences(DatabaseMetaData metaData, SequencePropertiesFactory factory) throws SQLException
	{
		ResultSet resultSet = metaData.getTables(Strings.EMPTY, null, Strings.ANY, new String[] { this.sequenceTableType() });
		
		try
		{
			List<SequenceProperties> sequences = new LinkedList<SequenceProperties>();
			
			while (resultSet.next())
			{
				sequences.add(factory.createSequenceProperties(resultSet.getString("TABLE_SCHEM"), resultSet.getString("TABLE_NAME"), 1));
			}
			
			return sequences;
		}
		finally
		{
			Resources.close(resultSet);
		}
	}

	protected String sequenceTableType()
	{
		return "SEQUENCE";
	}

	/**
	 * @see net.sf.hajdbc.SequenceSupport#getNextSequenceValueSQL(net.sf.hajdbc.SequenceProperties)
	 */
	@Override
	public String getNextSequenceValueSQL(SequenceProperties sequence)
	{
		return this.executeFunctionSQL(MessageFormat.format(this.nextSequenceValueFormat(), sequence.getName().getDMLName()));
	}
	
	protected String nextSequenceValueFormat()
	{
		return "NEXT VALUE FOR {0}";
	}
	
	/**
	 * @see net.sf.hajdbc.SequenceSupport#getAlterSequenceSQL(net.sf.hajdbc.SequenceProperties, long)
	 */
	@Override
	public String getAlterSequenceSQL(SequenceProperties sequence, long value)
	{
		return MessageFormat.format(this.alterSequenceFormat(), sequence.getName().getDDLName(), String.valueOf(value), String.valueOf(sequence.getIncrement()));
	}
	
	protected String alterSequenceFormat()
	{
		return "ALTER SEQUENCE {0} RESTART WITH {1}";
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.dialect.Dialect#getIdentityColumnSupport()
	 */
	@Override
	public IdentityColumnSupport getIdentityColumnSupport()
	{
		return null;
	}
	
	@Override
	public String parseInsertTable(String sql)
	{
		return this.parse(this.insertIntoTablePattern, sql);
	}

	@Override
	public String getAlterIdentityColumnSQL(TableProperties table, ColumnProperties column, long value) throws SQLException
	{
		return MessageFormat.format(this.alterIdentityColumnFormat(), table.getName().getDDLName(), column.getName(), String.valueOf(value));
	}

	protected String alterIdentityColumnFormat()
	{
		return "ALTER TABLE {0} ALTER COLUMN {1} RESTART WITH {2}";
	}

	protected String parse(Pattern pattern, String string)
	{
		Matcher matcher = pattern.matcher(string);
		
		return matcher.find() ? matcher.group(1) : null;
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.dialect.Dialect#evaluateCurrentDate(java.lang.String, java.sql.Date)
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
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.dialect.Dialect#evaluateCurrentTime(java.lang.String, java.sql.Time)
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
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.dialect.Dialect#evaluateCurrentTimestamp(java.lang.String, java.sql.Timestamp)
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
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.dialect.Dialect#evaluateRand(java.lang.String)
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

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.dialect.Dialect#indicatesFailure(java.sql.SQLException)
	 */
	@Override
	public boolean indicatesFailure(SQLException e)
	{
		String state = e.getSQLState();
		Set<String> failureSQLStates = this.failureSQLStates();
		
		if ((state != null) && !failureSQLStates.isEmpty())
		{
			return failureSQLStates.contains(state);
		}
		
		return (e instanceof SQLNonTransientConnectionException);
	}

	protected Set<String> failureSQLStates()
	{
		return Collections.emptySet();
	}
	
	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.dialect.Dialect#indicatesFailure(javax.transaction.xa.XAException)
	 */
	@Override
	public boolean indicatesFailure(XAException e)
	{
		return this.failureXAErrorCodes().contains(e.errorCode);
	}
	
	protected Set<Integer> failureXAErrorCodes()
	{
		return Collections.singleton(XAException.XAER_RMFAIL);
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.dialect.Dialect#getUrlPattern()
	 */
	@Override
	public Pattern getUrlPattern()
	{
		return this.urlPattern;
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.dialect.Dialect#getDumpRestoreSupport()
	 */
	@Override
	public DumpRestoreSupport getDumpRestoreSupport()
	{
		return null;
	}
	
	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.dialect.Dialect#getTriggerSupport()
	 */
	@Override
	public TriggerSupport getTriggerSupport()
	{
		return null;
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.TriggerSupport#getCreateTriggerSQL(java.lang.String, net.sf.hajdbc.TableProperties, net.sf.hajdbc.TriggerEvent, java.lang.String)
	 */
	@Override
	public String getCreateTriggerSQL(String name, TableProperties table, TriggerEvent event, String action)
	{
		return MessageFormat.format(this.createTriggerFormat(), name, event.getTime().toString(), event.toString(), table.getName().getDDLName(), action);
	}

	protected String createTriggerFormat()
	{
		return "CREATE TRIGGER {0} {1} {2} ON {3} FOR EACH ROW BEGIN {4} END";
	}
	
	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.TriggerSupport#getDropTriggerSQL(String, net.sf.hajdbc.TableProperties)
	 */
	@Override
	public String getDropTriggerSQL(String name, TableProperties table)
	{
		return MessageFormat.format(this.dropTriggerFormat(), name, table.getName().getDDLName());
	}

	protected String dropTriggerFormat()
	{
		return "DROP TRIGGER {1} ON {2}";
	}
	
	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.TriggerSupport#getTriggerRowAlias(net.sf.hajdbc.TriggerTime)
	 */
	@Override
	public String getTriggerRowAlias(TriggerTime time)
	{
		return time.getAlias();
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.dialect.Dialect#getCreateSchemaSQL(java.lang.String)
	 */
	@Override
	public String getCreateSchemaSQL(String schema)
	{
		return MessageFormat.format(this.createSchemaFormat(), schema);
	}

	protected String createSchemaFormat()
	{
		return "CREATE SCHEMA {0}";
	}
	
	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.dialect.Dialect#getDropSchemaSQL(java.lang.String)
	 */
	@Override
	public String getDropSchemaSQL(String schema)
	{
		return MessageFormat.format(this.dropSchemaFormat(), schema);
	}

	protected String dropSchemaFormat()
	{
		return "DROP SCHEMA {0}";
	}

	protected boolean meetsRequirement(int minMajor, int minMinor)
	{
		Driver driver = this.findDriver();

		if (driver != null)
		{
			int major = driver.getMajorVersion();
			int minor = driver.getMinorVersion();
			return (major > minMajor) || ((major == minMajor) && (minor >= minMinor));
		}
		
		return false;
	}
	
	private Driver findDriver()
	{
		String url = String.format("jdbc:%s:test", this.vendorPattern());
		
		List<Driver> drivers = Collections.list(DriverManager.getDrivers());
		for (Driver driver: drivers)
		{
			try
			{
				if (driver.acceptsURL(url))
				{
					return driver;
				}
			}
			catch (SQLException e)
			{
				// Skip
			}
		}
		return null;
	}
	
	/**
	 * Returns all tables in this database mapped by schema.
	 * @param metaData a DatabaseMetaData implementation
	 * @return a Map of schema name to Collection of table names
	 * @throws SQLException if an error occurs access DatabaseMetaData
	 */
	@Override
	public Collection<QualifiedName> getTables(DatabaseMetaData metaData, QualifiedNameFactory factory) throws SQLException
	{
		ResultSet resultSet = metaData.getTables(this.getCatalog(metaData), null, Strings.ANY, new String[] { "TABLE" });
		
		try
		{
			List<QualifiedName> list = new LinkedList<QualifiedName>();
			
			while (resultSet.next())
			{
				list.add(factory.createQualifiedName(resultSet.getString("TABLE_SCHEM"), resultSet.getString("TABLE_NAME")));
			}
			
			return list;
		}
		finally
		{
			Resources.close(resultSet);
		}
	}

	/**
	 * Returns the columns of the specified table.
	 * @param metaData a DatabaseMetaData implementation
	 * @param table a schema qualified table name
	 * @return a Map of column name to column properties
	 * @throws SQLException if an error occurs access DatabaseMetaData
	 */
	@Override
	public Map<String, ColumnProperties> getColumns(DatabaseMetaData metaData, QualifiedName table, ColumnPropertiesFactory factory) throws SQLException
	{
		Statement statement = metaData.getConnection().createStatement();
		
		try
		{
			Map<String, ColumnProperties> map = new HashMap<String, ColumnProperties>();
			
			ResultSetMetaData resultSet = statement.executeQuery(String.format("SELECT * FROM %s WHERE 0=1", table.getDMLName())).getMetaData();
			
			for (int i = 1; i <= resultSet.getColumnCount(); ++i)
			{
				String column = resultSet.getColumnName(i);
				int type = resultSet.getColumnType(i);
				String nativeType = resultSet.getColumnTypeName(i);
				boolean autoIncrement = resultSet.isAutoIncrement(i);
				
				ColumnProperties properties = factory.createColumnProperties(column, type, nativeType, null, null, autoIncrement);
				map.put(properties.getName(), properties);
			}
			
			return map;
		}
		finally
		{
			Resources.close(statement);
		}
	}

	/**
	 * Returns the primary key of the specified table.
	 * @param metaData a DatabaseMetaData implementation
	 * @param table a schema qualified table name
	 * @return a unique constraint
	 * @throws SQLException if an error occurs access DatabaseMetaData
	 */
	@Override
	public UniqueConstraint getPrimaryKey(DatabaseMetaData metaData, QualifiedName table, UniqueConstraintFactory factory) throws SQLException
	{
		ResultSet resultSet = metaData.getPrimaryKeys(this.getCatalog(metaData), table.getSchema(), table.getName());
		
		try
		{
			UniqueConstraint constraint = null;

			while (resultSet.next())
			{
				if (constraint == null)
				{
					constraint = factory.createUniqueConstraint(resultSet.getString("PK_NAME"), table);
				}
				
				constraint.getColumnList().add(resultSet.getString("COLUMN_NAME"));
			}
			
			return constraint;
		}
		finally
		{
			Resources.close(resultSet);
		}
	}

	/**
	 * Returns the foreign key constraints on the specified table.
	 * @param metaData a DatabaseMetaData implementation
	 * @param table a schema qualified table name
	 * @return a Collection of foreign key constraints.
	 * @throws SQLException if an error occurs access DatabaseMetaData
	 */
	@Override
	public Collection<ForeignKeyConstraint> getForeignKeyConstraints(DatabaseMetaData metaData, QualifiedName table, ForeignKeyConstraintFactory factory) throws SQLException
	{
		ResultSet resultSet = metaData.getImportedKeys(this.getCatalog(metaData), table.getSchema(), table.getName());
		
		try
		{
			Map<String, ForeignKeyConstraint> foreignKeyMap = new HashMap<String, ForeignKeyConstraint>();
			
			while (resultSet.next())
			{
				String name = resultSet.getString("FK_NAME");
				
				ForeignKeyConstraint foreignKey = foreignKeyMap.get(name);
				
				if (foreignKey == null)
				{
					foreignKey = factory.createForeignKeyConstraint(name, table, factory.getQualifiedNameFactory().createQualifiedName(resultSet.getString("PKTABLE_SCHEM"), resultSet.getString("PKTABLE_NAME")), resultSet.getInt("DELETE_RULE"), resultSet.getInt("UPDATE_RULE"), resultSet.getInt("DEFERRABILITY"));
					
					foreignKeyMap.put(name, foreignKey);
				}
				
				foreignKey.getColumnList().add(resultSet.getString("FKCOLUMN_NAME"));
				foreignKey.getForeignColumnList().add(resultSet.getString("PKCOLUMN_NAME"));
			}
			
			return foreignKeyMap.values();
		}
		finally
		{
			Resources.close(resultSet);
		}
	}

	/**
	 * Returns the unique constraints on the specified table - excluding the primary key of the table.
	 * @param metaData a schema qualified table name
	 * @param table a qualified table name
	 * @param primaryKey the primary key of this table
	 * @return a Collection of unique constraints.
	 * @throws SQLException if an error occurs access DatabaseMetaData
	 */
	@Override
	public Collection<UniqueConstraint> getUniqueConstraints(DatabaseMetaData metaData, QualifiedName table, UniqueConstraint primaryKey, UniqueConstraintFactory factory) throws SQLException
	{
		ResultSet resultSet = metaData.getIndexInfo(this.getCatalog(metaData), table.getSchema(), table.getName(), true, false);
		
		try
		{
			Map<String, UniqueConstraint> keyMap = new HashMap<String, UniqueConstraint>();
			
			while (resultSet.next())
			{
				if (resultSet.getShort("TYPE") == DatabaseMetaData.tableIndexHashed)
				{
					String name = resultSet.getString("INDEX_NAME");
					
					UniqueConstraint key = keyMap.get(name);
					
					if (key == null)
					{
						key = factory.createUniqueConstraint(name, table);
						
						// Don't include the primary key
						if (key.equals(primaryKey)) continue;
						
						keyMap.put(name, key);
					}
					
					key.getColumnList().add(resultSet.getString("COLUMN_NAME"));
				}
			}
			return keyMap.values();
		}
		finally
		{
			Resources.close(resultSet);
		}
	}
	
	private String getCatalog(DatabaseMetaData metaData) throws SQLException
	{
		String catalog = metaData.getConnection().getCatalog();
		
		return (catalog != null) ? catalog : Strings.EMPTY;
	}
	
	/**
	 * Identifies any identity columns from the from the specified collection of columns
	 * @param columns the columns of a table
	 * @return a collection of column names
	 * @throws SQLException
	 */
	@Override
	public Collection<String> getIdentityColumns(Collection<ColumnProperties> columns) throws SQLException
	{
		List<String> columnList = new LinkedList<String>();
		
		for (ColumnProperties column: columns)
		{
			if (column.isAutoIncrement())
			{
				columnList.add(column.getName());
			}
		}
		
		return columnList;
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.dialect.Dialect#getTypes(java.sql.DatabaseMetaData)
	 */
	@Override
	public Map<Integer, Entry<String, Integer>> getTypes(DatabaseMetaData metaData) throws SQLException
	{
		ResultSet resultSet = metaData.getTypeInfo();
		
		try
		{
			Map<Integer, Map.Entry<String, Integer>> types = new HashMap<Integer, Map.Entry<String, Integer>>();
			
			while (resultSet.next())
			{
				int type = resultSet.getInt("DATA_TYPE");
				if (!types.containsKey(type))
				{
					String name = resultSet.getString("TYPE_NAME");
					String params = resultSet.getString("CREATE_PARAMS");
					types.put(type, new AbstractMap.SimpleImmutableEntry<String, Integer>(name, (params != null) ? resultSet.getInt("PRECISION") : null));
				}
			}
			
			return types;
		}
		finally
		{
			Resources.close(resultSet);
		}
	}

	@Override
	public IdentifierNormalizer createIdentifierNormalizer(DatabaseMetaData metaData) throws SQLException
	{
		return new StandardIdentifierNormalizer(metaData, Pattern.compile(MessageFormat.format("[a-zA-Z][\\w{0}]*", Pattern.quote(metaData.getExtraNameCharacters()))));
	}

	@Override
	public QualifiedNameFactory createQualifiedNameFactory(DatabaseMetaData metaData, IdentifierNormalizer normalizer) throws SQLException
	{
		return new StandardQualifiedNameFactory(metaData, normalizer);
	}

	@Override
	public ColumnPropertiesFactory createColumnPropertiesFactory(IdentifierNormalizer normalizer)
	{
		return new StandardColumnPropertiesFactory(normalizer);
	}

	@Override
	public SequencePropertiesFactory createSequencePropertiesFactory(QualifiedNameFactory factory)
	{
		return new StandardSequencePropertiesFactory(factory);
	}

	@Override
	public ForeignKeyConstraintFactory createForeignKeyConstraintFactory(QualifiedNameFactory factory)
	{
		return new StandardForeignKeyConstraintFactory(factory);
	}

	@Override
	public UniqueConstraintFactory createUniqueConstraintFactory(IdentifierNormalizer normalizer)
	{
		return new StandardUniqueConstraintFactory(normalizer);
	}
}
