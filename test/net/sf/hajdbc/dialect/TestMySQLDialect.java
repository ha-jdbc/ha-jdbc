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
import java.util.Collection;
import java.util.List;

import net.sf.hajdbc.ColumnProperties;
import net.sf.hajdbc.ForeignKeyConstraint;
import net.sf.hajdbc.QualifiedName;
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
public class TestMySQLDialect extends TestStandardDialect
{
	public TestMySQLDialect()
	{
		super(new MySQLDialect());
	}

	/**
	 * @see net.sf.hajdbc.dialect.TestStandardDialect#testGetCreateForeignKeyConstraintSQL()
	 */
	@Override
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
		
		assert result.equals("ALTER TABLE table ADD CONSTRAINT name FOREIGN KEY (column1, column2) REFERENCES foreign_table (foreign_column1, foreign_column2) ON DELETE CASCADE ON UPDATE RESTRICT") : result;
	}

	/**
	 * @see net.sf.hajdbc.dialect.TestStandardDialect#testGetDropForeignKeyConstraintSQL()
	 */
	@Override
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
		
		assert result.equals("ALTER TABLE table DROP FOREIGN KEY name") : result;
	}

	/**
	 * @see net.sf.hajdbc.dialect.TestStandardDialect#testGetCreateUniqueConstraintSQL()
	 */
	@Override
	public void testGetCreateUniqueConstraintSQL() throws SQLException
	{
		UniqueConstraint key = new UniqueConstraintImpl("name", "table");
		key.getColumnList().add("column1");
		key.getColumnList().add("column2");
		
		String result = this.getCreateUniqueConstraintSQL(key);
		
		assert result.equals("ALTER TABLE table ADD UNIQUE name (column1, column2)") : result;
	}

	/**
	 * @see net.sf.hajdbc.dialect.TestStandardDialect#testGetDropUniqueConstraintSQL()
	 */
	@Override
	public void testGetDropUniqueConstraintSQL() throws SQLException
	{
		UniqueConstraint key = new UniqueConstraintImpl("name", "table");
		key.getColumnList().add("column1");
		key.getColumnList().add("column2");
		
		String result = this.getDropUniqueConstraintSQL(key);
		
		assert result.equals("ALTER TABLE table DROP INDEX name") : result;
	}

	/**
	 * @see net.sf.hajdbc.dialect.TestStandardDialect#testParseSequence(java.lang.String)
	 */
	@Override
	@Test(dataProvider = "sequence-sql")
	public void testParseSequence(String sql) throws SQLException
	{
		String result = this.parseSequence(sql);
		
		assert (result == null) : result;
	}

	/**
	 * @see net.sf.hajdbc.dialect.TestStandardDialect#testGetSequences()
	 */
	@Override
	public void testGetSequences() throws SQLException
	{
		DatabaseMetaData metaData = EasyMock.createStrictMock(DatabaseMetaData.class);
		
		Collection<QualifiedName> result = this.getSequences(metaData);
		
		assert result.isEmpty() : result;
	}

	/**
	 * @see net.sf.hajdbc.dialect.TestStandardDialect#testGetDefaultSchemas()
	 */
	@Override
	public void testGetDefaultSchemas() throws SQLException
	{
		DatabaseMetaData metaData = EasyMock.createStrictMock(DatabaseMetaData.class);
		Connection connection = EasyMock.createStrictMock(Connection.class);
		Statement statement = EasyMock.createStrictMock(Statement.class);
		ResultSet resultSet = EasyMock.createStrictMock(ResultSet.class);
		
		EasyMock.expect(metaData.getConnection()).andReturn(connection);
		EasyMock.expect(connection.createStatement()).andReturn(statement);
		EasyMock.expect(statement.executeQuery("SELECT DATABASE()")).andReturn(resultSet);
		EasyMock.expect(resultSet.next()).andReturn(false);
		EasyMock.expect(resultSet.getString(1)).andReturn("database");

		resultSet.close();
		statement.close();
		
		EasyMock.replay(metaData, connection, statement, resultSet);
		
		List<String> result = this.getDefaultSchemas(metaData);
		
		EasyMock.verify(metaData, connection, statement, resultSet);
		
		assert result.size() == 1 : result.size();
		
		assert result.get(0).equals("database") : result.get(0);
	}

	/**
	 * @see net.sf.hajdbc.dialect.TestStandardDialect#testIsIdentity()
	 */
	@Override
	public void testIsIdentity() throws SQLException
	{
		ColumnProperties column = EasyMock.createStrictMock(ColumnProperties.class);
		
		EasyMock.expect(column.getNativeType()).andReturn("SERIAL");
		
		EasyMock.replay(column);
		
		boolean result = this.isIdentity(column);
		
		EasyMock.verify(column);
		
		assert result;
			
		EasyMock.reset(column);
		
		EasyMock.expect(column.getNativeType()).andReturn("INTEGER");
		EasyMock.expect(column.getRemarks()).andReturn("AUTO_INCREMENT");
		
		EasyMock.replay(column);
		
		result = this.isIdentity(column);
		
		EasyMock.verify(column);
		
		assert result;
		
		EasyMock.reset(column);
		
		EasyMock.expect(column.getNativeType()).andReturn("INTEGER");
		EasyMock.expect(column.getRemarks()).andReturn(null);
		
		EasyMock.replay(column);
		
		result = this.isIdentity(column);
		
		EasyMock.verify(column);
		
		assert !result;
	}
	
	/**
	 * @see net.sf.hajdbc.dialect.TestStandardDialect#testGetAlterIdentityColumnSQL()
	 */
	@Override
	public void testGetAlterIdentityColumnSQL() throws SQLException
	{
		TableProperties table = EasyMock.createStrictMock(TableProperties.class);
		ColumnProperties column = EasyMock.createStrictMock(ColumnProperties.class);
		
		EasyMock.expect(table.getName()).andReturn("table");
		EasyMock.expect(column.getName()).andReturn("column");
		
		EasyMock.replay(table, column);
		
		String result = this.getAlterIdentityColumnSQL(table, column, 1000L);
		
		EasyMock.verify(table, column);
		
		assert result.equals("ALTER TABLE table AUTO_INCREMENT = 1000") : result;
	}

	@Override
	@DataProvider(name = "current-date")
	Object[][] currentDateProvider()
	{
		java.sql.Date date = new java.sql.Date(System.currentTimeMillis());
		
		return new Object[][] {
			new Object[] { "SELECT CURRENT_DATE FROM success", date },
			new Object[] { "SELECT CURDATE() FROM success", date },
			new Object[] { "SELECT CURDATE ( ) FROM success", date },
			new Object[] { "SELECT CCURRENT_DATE FROM failure", date },
			new Object[] { "SELECT CURRENT_DATES FROM failure", date },
			new Object[] { "SELECT CCURDATE() FROM failure", date },
			new Object[] { "SELECT 1 FROM failure", date },
		};
	}
	
	@Override
	@Test(dataProvider = "current-date")
	public void testEvaluateCurrentDate(String sql, java.sql.Date date)
	{
		String expected = sql.contains("success") ? String.format("SELECT '%s' FROM success", date.toString()) : sql;
		
		String evaluated = this.evaluateCurrentDate(sql, date);

		assert evaluated.equals(expected) : evaluated;
	}

	@Override
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
			new Object[] { "SELECT CURTIME() FROM success", date },
			new Object[] { "SELECT CURTIME ( ) FROM success", date },
			new Object[] { "SELECT CCURRENT_TIME FROM failure", date },
			new Object[] { "SELECT CURRENT_TIMESTAMP FROM failure", date },
			new Object[] { "SELECT LLOCALTIME FROM failure", date },
			new Object[] { "SELECT LOCALTIMESTAMP FROM failure", date },
			new Object[] { "SELECT CCURTIME() FROM failure", date },
			new Object[] { "SELECT 1 FROM failure", date },
		};
	}
	
	@Override
	@Test(dataProvider = "current-time")
	public void testEvaluateCurrentTime(String sql, java.sql.Time date)
	{
		String expected = sql.contains("success") ? String.format("SELECT '%s' FROM success", date.toString()) : sql;
		
		String evaluated = this.evaluateCurrentTime(sql, date);

		assert evaluated.equals(expected) : evaluated;
	}

	@Override
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
			new Object[] { "SELECT NOW() FROM success", date },
			new Object[] { "SELECT NOW ( ) FROM success", date },
			new Object[] { "SELECT SYSDATE() FROM success", date },
			new Object[] { "SELECT SYSDATE ( ) FROM success", date },
			new Object[] { "SELECT CCURRENT_TIMESTAMP FROM failure", date },
			new Object[] { "SELECT CURRENT_TIMESTAMPS FROM failure", date },
			new Object[] { "SELECT LLOCALTIMESTAMP FROM failure", date },
			new Object[] { "SELECT LOCALTIMESTAMPS FROM failure", date },
			new Object[] { "SELECT NNOW() FROM failure", date },
			new Object[] { "SELECT SSYSDATE() FROM failure", date },
			new Object[] { "SELECT 1 FROM failure", date },
		};
	}
	
	@Override
	@Test(dataProvider = "current-timestamp")
	public void testEvaluateCurrentTimestamp(String sql, java.sql.Timestamp date)
	{
		String expected = sql.contains("success") ? String.format("SELECT '%s' FROM success", date.toString()) : sql;
		
		String evaluated = this.evaluateCurrentTimestamp(sql, date);

		assert evaluated.equals(expected) : evaluated;
	}
}
