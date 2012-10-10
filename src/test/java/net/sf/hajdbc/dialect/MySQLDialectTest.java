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
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import net.sf.hajdbc.ForeignKeyConstraint;
import net.sf.hajdbc.QualifiedName;
import net.sf.hajdbc.UniqueConstraint;
import net.sf.hajdbc.cache.ForeignKeyConstraintImpl;
import net.sf.hajdbc.cache.UniqueConstraintImpl;
import net.sf.hajdbc.dialect.mysql.MySQLDialectFactory;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 * @author Paul Ferraro
 *
 */
@SuppressWarnings("nls")
public class MySQLDialectTest extends StandardDialectTest
{
	public MySQLDialectTest()
	{
		super(new MySQLDialectFactory());
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.dialect.StandardDialectTest#getCreateForeignKeyConstraintSQL()
	 */
	@Override
	public void getCreateForeignKeyConstraintSQL() throws SQLException
	{
		QualifiedName table = mock(QualifiedName.class);
		QualifiedName foreignTable = mock(QualifiedName.class);
		
		when(table.getDDLName()).thenReturn("table");
		when(foreignTable.getDDLName()).thenReturn("foreign_table");
		
		ForeignKeyConstraint key = new ForeignKeyConstraintImpl("name", table);
		key.getColumnList().add("column1");
		key.getColumnList().add("column2");
		key.setForeignTable(foreignTable);
		key.getForeignColumnList().add("foreign_column1");
		key.getForeignColumnList().add("foreign_column2");
		key.setDeferrability(DatabaseMetaData.importedKeyInitiallyDeferred);
		key.setDeleteRule(DatabaseMetaData.importedKeyCascade);
		key.setUpdateRule(DatabaseMetaData.importedKeyRestrict);
		
		String result = this.dialect.getCreateForeignKeyConstraintSQL(key);
		
		assertEquals("ALTER TABLE table ADD CONSTRAINT name FOREIGN KEY (column1, column2) REFERENCES foreign_table (foreign_column1, foreign_column2) ON DELETE CASCADE ON UPDATE RESTRICT", result);
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.dialect.StandardDialectTest#getDropForeignKeyConstraintSQL()
	 */
	@Override
	public void getDropForeignKeyConstraintSQL() throws SQLException
	{
		QualifiedName table = mock(QualifiedName.class);
		QualifiedName foreignTable = mock(QualifiedName.class);
		
		when(table.getDDLName()).thenReturn("table");
		when(foreignTable.getDDLName()).thenReturn("foreign_table");
		
		ForeignKeyConstraint key = new ForeignKeyConstraintImpl("name", table);
		key.getColumnList().add("column1");
		key.getColumnList().add("column2");
		key.setForeignTable(foreignTable);
		key.getForeignColumnList().add("foreign_column1");
		key.getForeignColumnList().add("foreign_column2");
		key.setDeferrability(DatabaseMetaData.importedKeyInitiallyDeferred);
		key.setDeleteRule(DatabaseMetaData.importedKeyCascade);
		key.setUpdateRule(DatabaseMetaData.importedKeyRestrict);
		
		String result = this.dialect.getDropForeignKeyConstraintSQL(key);
		
		assertEquals("ALTER TABLE table DROP FOREIGN KEY name", result);
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.dialect.StandardDialectTest#getCreateUniqueConstraintSQL()
	 */
	@Override
	public void getCreateUniqueConstraintSQL() throws SQLException
	{
		QualifiedName table = mock(QualifiedName.class);
		
		when(table.getDDLName()).thenReturn("table");
		
		UniqueConstraint key = new UniqueConstraintImpl("name", table);
		key.getColumnList().add("column1");
		key.getColumnList().add("column2");
		
		String result = this.dialect.getCreateUniqueConstraintSQL(key);
		
		assertEquals("ALTER TABLE table ADD UNIQUE name (column1, column2)", result);
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.dialect.StandardDialectTest#getDropUniqueConstraintSQL()
	 */
	@Override
	public void getDropUniqueConstraintSQL() throws SQLException
	{
		QualifiedName table = mock(QualifiedName.class);
		
		when(table.getDDLName()).thenReturn("table");
		
		UniqueConstraint key = new UniqueConstraintImpl("name", table);
		key.getColumnList().add("column1");
		key.getColumnList().add("column2");
		
		String result = this.dialect.getDropUniqueConstraintSQL(key);
		
		assertEquals("ALTER TABLE table DROP INDEX name", result);
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.dialect.StandardDialectTest#getDefaultSchemas()
	 */
	@Override
	public void getDefaultSchemas() throws SQLException
	{
		DatabaseMetaData metaData = mock(DatabaseMetaData.class);
		Connection connection = mock(Connection.class);
		Statement statement = mock(Statement.class);
		ResultSet resultSet = mock(ResultSet.class);
		
		when(metaData.getConnection()).thenReturn(connection);
		when(connection.createStatement()).thenReturn(statement);
		when(statement.executeQuery("SELECT DATABASE()")).thenReturn(resultSet);
		when(resultSet.next()).thenReturn(false);
		when(resultSet.getString(1)).thenReturn("database");

		List<String> result = this.dialect.getDefaultSchemas(metaData);

		verify(statement).close();
		
		assertEquals(1, result.size());
		assertEquals("database", result.get(0));
	}
	
	@Override
	public void evaluateCurrentDate()
	{
		java.sql.Date date = new java.sql.Date(System.currentTimeMillis());
		
		assertEquals(String.format("SELECT '%s' FROM test", date.toString()), this.dialect.evaluateCurrentDate("SELECT CURRENT_DATE FROM test", date));
		assertEquals(String.format("SELECT '%s' FROM test", date.toString()), this.dialect.evaluateCurrentDate("SELECT CURDATE() FROM test", date));
		assertEquals(String.format("SELECT '%s' FROM test", date.toString()), this.dialect.evaluateCurrentDate("SELECT CURDATE ( ) FROM test", date));
		assertEquals("SELECT CCURRENT_DATE FROM test", this.dialect.evaluateCurrentDate("SELECT CCURRENT_DATE FROM test", date));
		assertEquals("SELECT CURRENT_DATES FROM test", this.dialect.evaluateCurrentDate("SELECT CURRENT_DATES FROM test", date));
		assertEquals("SELECT CCURDATE() FROM test", this.dialect.evaluateCurrentDate("SELECT CCURDATE() FROM test", date));
		assertEquals("SELECT CURRENT_TIME FROM test", this.dialect.evaluateCurrentDate("SELECT CURRENT_TIME FROM test", date));
		assertEquals("SELECT CURRENT_TIMESTAMP FROM test", this.dialect.evaluateCurrentDate("SELECT CURRENT_TIMESTAMP FROM test", date));
		assertEquals("SELECT 1 FROM test", this.dialect.evaluateCurrentDate("SELECT 1 FROM test", date));
	}
	
	@Override
	public void evaluateCurrentTime()
	{
		java.sql.Time time = new java.sql.Time(System.currentTimeMillis());
		
		assertEquals(String.format("SELECT '%s' FROM test", time.toString()), this.dialect.evaluateCurrentTime("SELECT CURRENT_TIME FROM test", time));
		assertEquals(String.format("SELECT '%s' FROM test", time.toString()), this.dialect.evaluateCurrentTime("SELECT CURRENT_TIME(2) FROM test", time));
		assertEquals(String.format("SELECT '%s' FROM test", time.toString()), this.dialect.evaluateCurrentTime("SELECT CURRENT_TIME ( 2 ) FROM test", time));
		assertEquals(String.format("SELECT '%s' FROM test", time.toString()), this.dialect.evaluateCurrentTime("SELECT LOCALTIME FROM test", time));
		assertEquals(String.format("SELECT '%s' FROM test", time.toString()), this.dialect.evaluateCurrentTime("SELECT LOCALTIME(2) FROM test", time));
		assertEquals(String.format("SELECT '%s' FROM test", time.toString()), this.dialect.evaluateCurrentTime("SELECT LOCALTIME ( 2 ) FROM test", time));
		assertEquals(String.format("SELECT '%s' FROM test", time.toString()), this.dialect.evaluateCurrentTime("SELECT CURTIME() FROM test", time));
		assertEquals(String.format("SELECT '%s' FROM test", time.toString()), this.dialect.evaluateCurrentTime("SELECT CURTIME ( ) FROM test", time));
		assertEquals("SELECT CCURRENT_TIME FROM test", this.dialect.evaluateCurrentTime("SELECT CCURRENT_TIME FROM test", time));
		assertEquals("SELECT LLOCALTIME FROM test", this.dialect.evaluateCurrentTime("SELECT LLOCALTIME FROM test", time));
		assertEquals("SELECT CURRENT_DATE FROM test", this.dialect.evaluateCurrentTime("SELECT CURRENT_DATE FROM test", time));
		assertEquals("SELECT CURRENT_TIMESTAMP FROM test", this.dialect.evaluateCurrentTime("SELECT CURRENT_TIMESTAMP FROM test", time));
		assertEquals("SELECT LOCALTIMESTAMP FROM test", this.dialect.evaluateCurrentTime("SELECT LOCALTIMESTAMP FROM test", time));
		assertEquals("SELECT 1 FROM test", this.dialect.evaluateCurrentTime("SELECT 1 FROM test", time));
	}
	
	@Override
	public void evaluateCurrentTimestamp()
	{
		java.sql.Timestamp timestamp = new java.sql.Timestamp(System.currentTimeMillis());
		
		assertEquals(String.format("SELECT '%s' FROM test", timestamp.toString()), this.dialect.evaluateCurrentTimestamp("SELECT CURRENT_TIMESTAMP FROM test", timestamp));
		assertEquals(String.format("SELECT '%s' FROM test", timestamp.toString()), this.dialect.evaluateCurrentTimestamp("SELECT CURRENT_TIMESTAMP(2) FROM test", timestamp));
		assertEquals(String.format("SELECT '%s' FROM test", timestamp.toString()), this.dialect.evaluateCurrentTimestamp("SELECT CURRENT_TIMESTAMP ( 2 ) FROM test", timestamp));
		assertEquals(String.format("SELECT '%s' FROM test", timestamp.toString()), this.dialect.evaluateCurrentTimestamp("SELECT LOCALTIMESTAMP FROM test", timestamp));
		assertEquals(String.format("SELECT '%s' FROM test", timestamp.toString()), this.dialect.evaluateCurrentTimestamp("SELECT LOCALTIMESTAMP(2) FROM test", timestamp));
		assertEquals(String.format("SELECT '%s' FROM test", timestamp.toString()), this.dialect.evaluateCurrentTimestamp("SELECT LOCALTIMESTAMP ( 2 ) FROM test", timestamp));
		assertEquals(String.format("SELECT '%s' FROM test", timestamp.toString()), this.dialect.evaluateCurrentTimestamp("SELECT NOW() FROM test", timestamp));
		assertEquals(String.format("SELECT '%s' FROM test", timestamp.toString()), this.dialect.evaluateCurrentTimestamp("SELECT NOW ( ) FROM test", timestamp));
		assertEquals(String.format("SELECT '%s' FROM test", timestamp.toString()), this.dialect.evaluateCurrentTimestamp("SELECT SYSDATE() FROM test", timestamp));
		assertEquals(String.format("SELECT '%s' FROM test", timestamp.toString()), this.dialect.evaluateCurrentTimestamp("SELECT SYSDATE ( ) FROM test", timestamp));
		assertEquals("SELECT CCURRENT_TIMESTAMP FROM test", this.dialect.evaluateCurrentTimestamp("SELECT CCURRENT_TIMESTAMP FROM test", timestamp));
		assertEquals("SELECT LLOCALTIMESTAMP FROM test", this.dialect.evaluateCurrentTimestamp("SELECT LLOCALTIMESTAMP FROM test", timestamp));
		assertEquals("SELECT CURRENT_DATE FROM test", this.dialect.evaluateCurrentTimestamp("SELECT CURRENT_DATE FROM test", timestamp));
		assertEquals("SELECT CURRENT_TIME FROM test", this.dialect.evaluateCurrentTimestamp("SELECT CURRENT_TIME FROM test", timestamp));
		assertEquals("SELECT LOCALTIME FROM test", this.dialect.evaluateCurrentTimestamp("SELECT LOCALTIME FROM test", timestamp));
		assertEquals("SELECT 1 FROM test", this.dialect.evaluateCurrentTimestamp("SELECT 1 FROM test", timestamp));
	}
}
