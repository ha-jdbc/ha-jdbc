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

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;

import net.sf.hajdbc.ColumnProperties;
import net.sf.hajdbc.Dialect;
import net.sf.hajdbc.ForeignKeyConstraint;
import net.sf.hajdbc.QualifiedName;
import net.sf.hajdbc.SequenceProperties;
import net.sf.hajdbc.TableProperties;
import net.sf.hajdbc.UniqueConstraint;
import net.sf.hajdbc.cache.ForeignKeyConstraintImpl;
import net.sf.hajdbc.cache.UniqueConstraintImpl;

import org.easymock.EasyMock;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * @author Paul Ferraro
 *
 */
@SuppressWarnings("nls")
@Test
public class TestStandardDialect implements Dialect
{
 	private Dialect dialect;
 	
	public TestStandardDialect()
	{
		this(new StandardDialect());
	}
	
	protected TestStandardDialect(Dialect dialect)
	{
		this.dialect = dialect;
	}

	public void testGetAlterSequenceSQL() throws SQLException
	{
		SequenceProperties sequence = EasyMock.createStrictMock(SequenceProperties.class);
		
		EasyMock.expect(sequence.getName()).andReturn("sequence");
		
		EasyMock.replay(sequence);
		
		String result = this.getAlterSequenceSQL(sequence, 1000L);

		EasyMock.verify(sequence);
		
		assert result.equals("ALTER SEQUENCE sequence RESTART WITH 1000") : result;
	}
	
	@Override
	public String getAlterSequenceSQL(SequenceProperties sequence, long value) throws SQLException
	{
		return this.dialect.getAlterSequenceSQL(sequence, value);
	}

	public void testGetColumnType() throws SQLException
	{
		ColumnProperties column = EasyMock.createStrictMock(ColumnProperties.class);
		
		EasyMock.expect(column.getType()).andReturn(Types.INTEGER);
		
		EasyMock.replay(column);
		
		int result = this.getColumnType(column);
		
		EasyMock.verify(column);
		
		assert result == Types.INTEGER : result;
	}
	
	@Override
	public int getColumnType(ColumnProperties column) throws SQLException
	{
		return this.dialect.getColumnType(column);
	}

	public void testGetCreateForeignKeyConstraintSQL() throws SQLException
	{
		ForeignKeyConstraint key = new ForeignKeyConstraintImpl("name", "table");
		key.getColumnList().add("column1");
		key.getColumnList().add("column2");
		key.setForeignTable("foreign_table");
		key.getForeignColumnList().add("foreign_column1");
		key.getForeignColumnList().add("foreign_column2");
		key.setDeferrability(DatabaseMetaData.importedKeyInitiallyDeferred);
		key.setDeleteRule(DatabaseMetaData.importedKeyCascade);
		key.setUpdateRule(DatabaseMetaData.importedKeyRestrict);
		
		String result = this.getCreateForeignKeyConstraintSQL(key);
		
		assert result.equals("ALTER TABLE table ADD CONSTRAINT name FOREIGN KEY (column1, column2) REFERENCES foreign_table (foreign_column1, foreign_column2) ON DELETE CASCADE ON UPDATE RESTRICT DEFERRABLE INITIALLY DEFERRED") : result;
	}
	
	@Override
	public String getCreateForeignKeyConstraintSQL(ForeignKeyConstraint constraint) throws SQLException
	{
		return this.dialect.getCreateForeignKeyConstraintSQL(constraint);
	}

	public void testGetCreateUniqueConstraintSQL() throws SQLException
	{
		UniqueConstraint key = new UniqueConstraintImpl("name", "table");
		key.getColumnList().add("column1");
		key.getColumnList().add("column2");
		
		String result = this.getCreateUniqueConstraintSQL(key);
		
		assert result.equals("ALTER TABLE table ADD CONSTRAINT name UNIQUE (column1, column2)") : result;
	}
	
	@Override
	public String getCreateUniqueConstraintSQL(UniqueConstraint constraint) throws SQLException
	{
		return this.dialect.getCreateUniqueConstraintSQL(constraint);
	}

	public void testGetDropForeignKeyConstraintSQL() throws SQLException
	{
		ForeignKeyConstraint key = new ForeignKeyConstraintImpl("name", "table");
		key.getColumnList().add("column1");
		key.getColumnList().add("column2");
		key.setForeignTable("foreign_table");
		key.getForeignColumnList().add("foreign_column1");
		key.getForeignColumnList().add("foreign_column2");
		key.setDeferrability(DatabaseMetaData.importedKeyInitiallyDeferred);
		key.setDeleteRule(DatabaseMetaData.importedKeyCascade);
		key.setUpdateRule(DatabaseMetaData.importedKeyRestrict);
		
		String result = this.getDropForeignKeyConstraintSQL(key);
		
		assert result.equals("ALTER TABLE table DROP CONSTRAINT name") : result;
	}
	
	@Override
	public String getDropForeignKeyConstraintSQL(ForeignKeyConstraint constraint) throws SQLException
	{
		return this.dialect.getDropForeignKeyConstraintSQL(constraint);
	}

	public void testGetDropUniqueConstraintSQL() throws SQLException
	{
		UniqueConstraint key = new UniqueConstraintImpl("name", "table");
		key.getColumnList().add("column1");
		key.getColumnList().add("column2");
		
		String result = this.getDropUniqueConstraintSQL(key);
		
		assert result.equals("ALTER TABLE table DROP CONSTRAINT name") : result;
	}
	
	@Override
	public String getDropUniqueConstraintSQL(UniqueConstraint constraint) throws SQLException
	{
		return this.dialect.getDropUniqueConstraintSQL(constraint);
	}

	public void testGetNextSequenceValueSQL() throws SQLException
	{
		SequenceProperties sequence = EasyMock.createStrictMock(SequenceProperties.class);
		
		EasyMock.expect(sequence.getName()).andReturn("sequence");
		
		EasyMock.replay(sequence);
		
		String result = this.getNextSequenceValueSQL(sequence);
		
		EasyMock.verify(sequence);
		
		assert result.equals("SELECT NEXT VALUE FOR sequence") : result;
	}
	
	@Override
	public String getNextSequenceValueSQL(SequenceProperties sequence) throws SQLException
	{
		return this.dialect.getNextSequenceValueSQL(sequence);
	}
	
	public void testGetSequences() throws SQLException
	{
		DatabaseMetaData metaData = EasyMock.createStrictMock(DatabaseMetaData.class);
		ResultSet resultSet = EasyMock.createStrictMock(ResultSet.class);
		
		EasyMock.expect(metaData.getTables(EasyMock.eq(""), EasyMock.eq((String) null), EasyMock.eq("%"), EasyMock.aryEq(new String[] { "SEQUENCE" }))).andReturn(resultSet);
		EasyMock.expect(resultSet.next()).andReturn(true);
		EasyMock.expect(resultSet.getString("TABLE_SCHEM")).andReturn("schema1");
		EasyMock.expect(resultSet.getString("TABLE_NAME")).andReturn("sequence1");
		EasyMock.expect(resultSet.next()).andReturn(true);
		EasyMock.expect(resultSet.getString("TABLE_SCHEM")).andReturn("schema2");
		EasyMock.expect(resultSet.getString("TABLE_NAME")).andReturn("sequence2");
		EasyMock.expect(resultSet.next()).andReturn(false);
		
		resultSet.close();
		
		EasyMock.replay(metaData, resultSet);
		
		Collection<QualifiedName> results = this.getSequences(metaData);
		
		EasyMock.verify(metaData, resultSet);
		
		assert results.size() == 2 : results;
		
		Iterator<QualifiedName> iterator = results.iterator();
		QualifiedName sequence = iterator.next();
		String schema = sequence.getSchema();
		String name = sequence.getName();
		
		assert schema.equals("schema1") : schema;
		assert name.equals("sequence1") : name;
		
		sequence = iterator.next();
		schema = sequence.getSchema();
		name = sequence.getName();
		
		assert schema.equals("schema2") : schema;
		assert name.equals("sequence2") : name;
	}
	
	@Override
	public Collection<QualifiedName> getSequences(DatabaseMetaData metaData) throws SQLException
	{
		return this.dialect.getSequences(metaData);
	}
	
	public void testGetSimpleSQL() throws SQLException
	{
		String result = this.getSimpleSQL();
		
		assert result.equals("SELECT CURRENT_TIMESTAMP") : result;
	}
	
	@Override
	public String getSimpleSQL() throws SQLException
	{
		return this.dialect.getSimpleSQL();
	}

	public void testGetTruncateTableSQL() throws SQLException
	{
		TableProperties table = EasyMock.createStrictMock(TableProperties.class);
		
		EasyMock.expect(table.getName()).andReturn("table");
		
		EasyMock.replay(table);
		
		String result = this.getTruncateTableSQL(table);
		
		EasyMock.verify(table);
		
		assert result.equals("DELETE FROM table") : result;
	}
	
	@Override
	public String getTruncateTableSQL(TableProperties properties) throws SQLException
	{
		return this.dialect.getTruncateTableSQL(properties);
	}

	@DataProvider(name = "select-for-update-sql")
	Object[][] selectForUpdateProvider()
	{
		return new Object[][] {
			new Object[] { "SELECT * FROM success FOR UPDATE" },
			new Object[] { "SELECT * FROM failure" },
		};
	}
	
	@Test(dataProvider = "select-for-update-sql")
	public void testIsSelectForUpdate(String sql) throws SQLException
	{
		boolean result = this.isSelectForUpdate(sql);
		
		assert result == sql.contains("success");
	}
	
	@Override
	public boolean isSelectForUpdate(String sql) throws SQLException
	{
		return this.dialect.isSelectForUpdate(sql);
	}

	@DataProvider(name = "sequence-sql")
	Object[][] sequenceSQLProvider()
	{
		return new Object[][] {
			new Object[] { "SELECT NEXT VALUE FOR success" },
			new Object[] { "SELECT NEXT VALUE FOR success, * FROM table" },
			new Object[] { "INSERT INTO table VALUES (NEXT VALUE FOR success, 0)" },
			new Object[] { "UPDATE table SET id = NEXT VALUE FOR success" },
			new Object[] { "SELECT * FROM table" },
		};
	}

	@Test(dataProvider = "sequence-sql")
	public void testParseSequence(String sql) throws SQLException
	{
		String result = this.parseSequence(sql);
		
		if (sql.contains("success"))
		{
			assert (result != null);
			assert result.equals("success") : result;
		}
		else
		{
			assert (result == null) : result;
		}
	}
	
	@Override
	public String parseSequence(String sql) throws SQLException
	{
		return this.dialect.parseSequence(sql);
	}

	public void testGetDefaultSchemas() throws SQLException
	{
		DatabaseMetaData metaData = EasyMock.createStrictMock(DatabaseMetaData.class);
		
		String user = "user";
		
		EasyMock.expect(metaData.getUserName()).andReturn(user);
		
		EasyMock.replay(metaData);
		
		List<String> result = this.getDefaultSchemas(metaData);
		
		EasyMock.verify(metaData);
		
		assert result.size() == 1 : result.size();
		
		String schema = result.get(0);
		
		assert schema.equals(user) : schema;
	}
	
	@Override
	public List<String> getDefaultSchemas(DatabaseMetaData metaData) throws SQLException
	{
		return this.dialect.getDefaultSchemas(metaData);
	}

	public void testIsIdentity() throws SQLException
	{
		ColumnProperties column = EasyMock.createStrictMock(ColumnProperties.class);
		
		EasyMock.expect(column.getRemarks()).andReturn("GENERATED BY DEFAULT AS IDENTITY");
		
		EasyMock.replay(column);
		
		boolean result = this.isIdentity(column);
		
		EasyMock.verify(column);
		
		assert result;
		
		EasyMock.reset(column);
		
		EasyMock.expect(column.getRemarks()).andReturn(null);
		
		EasyMock.replay(column);
		
		result = this.isIdentity(column);
		
		EasyMock.verify(column);
		
		assert !result;
	}
	
	@Override
	public boolean isIdentity(ColumnProperties properties) throws SQLException
	{
		return this.dialect.isIdentity(properties);
	}

	@DataProvider(name = "insert-table-sql")
	Object[][] insertTableProvider()
	{
		return new Object[][] { 
			new Object[] { "INSERT INTO success (column1, column2) VALUES (1, 2)" },
			new Object[] { "INSERT INTO success VALUES (1, 2)" },
			new Object[] { "INSERT success (column1, column2) VALUES (1, 2)" },
			new Object[] { "INSERT success VALUES (1, 2)" },
			new Object[] { "INSERT INTO success (column1, column2) SELECT column1, column2 FROM dummy" },
			new Object[] { "INSERT INTO success SELECT column1, column2 FROM dummy" },
			new Object[] { "INSERT success (column1, column2) SELECT column1, column2 FROM dummy" },
			new Object[] { "INSERT success SELECT column1, column2 FROM dummy" },
			new Object[] { "SELECT * FROM failure WHERE 0=1" },
			new Object[] { "UPDATE failure SET column = 0" },
		};
	}
	
	@Test(dataProvider = "insert-table-sql")
	public void testParseInsertTable(String sql) throws SQLException
	{
		String result = this.parseInsertTable(sql);
		
		if (sql.contains("success"))
		{
			assert result != null;
			assert result.equals("success");
		}
		else
		{
			assert result == null : result;
		}
	}
	
	@Override
	public String parseInsertTable(String sql) throws SQLException
	{
		return this.dialect.parseInsertTable(sql);
	}
	
	public void testGetIdentifierPattern() throws SQLException
	{
		DatabaseMetaData metaData = EasyMock.createStrictMock(DatabaseMetaData.class);
		
		EasyMock.expect(metaData.getExtraNameCharacters()).andReturn("-");
		
		EasyMock.replay(metaData);
		
		Pattern result = this.getIdentifierPattern(metaData);
		
		EasyMock.verify(metaData);
		
		assert result.pattern().equals("[\\w\\Q-\\E]+");
	}
	
	@Override
	public Pattern getIdentifierPattern(DatabaseMetaData metaData) throws SQLException
	{
		return this.dialect.getIdentifierPattern(metaData);
	}

	@DataProvider(name = "current-date")
	Object[][] currentDateProvider()
	{
		java.sql.Date date = new java.sql.Date(System.currentTimeMillis());
		
		return new Object[][] {
			new Object[] { "SELECT CURRENT_DATE FROM success", date },
			new Object[] { "SELECT CCURRENT_DATE FROM failure", date },
			new Object[] { "SELECT CURRENT_DATES FROM failure", date },
			new Object[] { "SELECT 1 FROM failure", date },
		};
	}
	
	@Test(dataProvider = "current-date")
	public void testEvaluateCurrentDate(String sql, java.sql.Date date) throws SQLException
	{
		String expected = sql.contains("success") ? String.format("SELECT DATE '%s' FROM success", date.toString()) : sql;
		
		String result = this.evaluateCurrentDate(sql, date);
		
		assert result.equals(expected) : result;
	}
	
	@Override
	public String evaluateCurrentDate(String sql, java.sql.Date date) throws SQLException
	{
		return this.dialect.evaluateCurrentDate(sql, date);
	}

	@DataProvider(name = "current-time")
	Object[][] currentTimeProvider()
	{
		java.sql.Time date = new java.sql.Time(System.currentTimeMillis());
		
		return new Object[][] {
			new Object[] { "SELECT CURRENT_TIME FROM success", date },
			new Object[] { "SELECT CURRENT_TIME(2) FROM success", date },
			new Object[] { "SELECT CURRENT_TIME ( 2 ) FROM success", date },
			new Object[] { "SELECT LOCALTIME FROM success", date },
			new Object[] { "SELECT LOCALTIME(2) FROM success", date },
			new Object[] { "SELECT LOCALTIME ( 2 ) FROM success", date },
			new Object[] { "SELECT CCURRENT_TIME FROM failure", date },
			new Object[] { "SELECT LLOCALTIME FROM failure", date },
			new Object[] { "SELECT CURRENT_TIMESTAMP FROM failure", date },
			new Object[] { "SELECT LOCALTIMESTAMP FROM failure", date },
			new Object[] { "SELECT 1 FROM failure", date },
		};
	}

	@Test(dataProvider = "current-time")
	public void testEvaluateCurrentTime(String sql, java.sql.Time date) throws SQLException
	{
		String expected = sql.contains("success") ? String.format("SELECT TIME '%s' FROM success", date.toString()) : sql;
		
		String result = this.evaluateCurrentTime(sql, date);
		
		assert result.equals(expected) : result;
	}
	
	@Override
	public String evaluateCurrentTime(String sql, java.sql.Time date) throws SQLException
	{
		return this.dialect.evaluateCurrentTime(sql, date);
	}

	@DataProvider(name = "current-timestamp")
	Object[][] currentTimestampProvider()
	{
		java.sql.Timestamp date = new java.sql.Timestamp(System.currentTimeMillis());
		
		return new Object[][] {
			new Object[] { "SELECT CURRENT_TIMESTAMP FROM success", date },
			new Object[] { "SELECT CURRENT_TIMESTAMP(2) FROM success", date },
			new Object[] { "SELECT CURRENT_TIMESTAMP ( 2 ) FROM success", date },
			new Object[] { "SELECT LOCALTIMESTAMP FROM success", date },
			new Object[] { "SELECT LOCALTIMESTAMP(2) FROM success", date },
			new Object[] { "SELECT LOCALTIMESTAMP ( 2 ) FROM success", date },
			new Object[] { "SELECT CURRENT_TIMESTAMPS FROM failure", date },
			new Object[] { "SELECT CCURRENT_TIMESTAMP FROM failure", date },
			new Object[] { "SELECT LOCALTIMESTAMPS FROM failure", date },
			new Object[] { "SELECT LLOCALTIMESTAMP FROM failure", date },
			new Object[] { "SELECT 1 FROM failure", date },
		};
	}

	@Test(dataProvider = "current-timestamp")
	public void testEvaluateCurrentTimestamp(String sql, java.sql.Timestamp date) throws SQLException
	{
		String expected = sql.contains("success") ? String.format("SELECT TIMESTAMP '%s' FROM success", date.toString()) : sql;
		
		String result = this.evaluateCurrentTimestamp(sql, date);
		
		assert result.equals(expected) : result;
	}
	
	@Override
	public String evaluateCurrentTimestamp(String sql, java.sql.Timestamp date) throws SQLException
	{
		return this.dialect.evaluateCurrentTimestamp(sql, date);
	}

	@DataProvider(name = "random")
	Object[][] randomProvider()
	{
		return new Object[][] {
			new Object[] { "SELECT RAND() FROM success" },
			new Object[] { "SELECT RAND ( ) FROM success" },
			new Object[] { "SELECT RAND FROM failure" },
			new Object[] { "SELECT OPERAND() FROM failure" },
			new Object[] { "SELECT 1 FROM failure" },
		};
	}
	
	@Test(dataProvider = "random")
	public void testEvaluateRand(String sql) throws SQLException
	{
		String result = this.evaluateRand(sql);
		
		if (sql.contains("success"))
		{
			assert Pattern.matches("SELECT 0\\.\\d+(E\\-\\d+)? FROM success", result) : result;
		}
		else
		{
			assert result.equals(sql) : result;
		}
	}
	
	@Override
	public String evaluateRand(String sql) throws SQLException
	{
		return this.dialect.evaluateRand(sql);
	}

	public void testGetAlterIdentityColumnSQL() throws SQLException
	{
		TableProperties table = EasyMock.createStrictMock(TableProperties.class);
		ColumnProperties column = EasyMock.createStrictMock(ColumnProperties.class);
		
		EasyMock.expect(table.getName()).andReturn("table");
		EasyMock.expect(column.getName()).andReturn("column");
		
		EasyMock.replay(table, column);
		
		String result = this.getAlterIdentityColumnSQL(table, column, 1000L);
		
		EasyMock.verify(table, column);
		
		assert result.equals("ALTER TABLE table ALTER COLUMN column RESTART WITH 1000") : result;
	}
	
	@Override
	public String getAlterIdentityColumnSQL(TableProperties table, ColumnProperties column, long value) throws SQLException
	{
		return this.dialect.getAlterIdentityColumnSQL(table, column, value);
	}
}
