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
import java.sql.SQLException;
import java.util.Arrays;

import net.sf.hajdbc.cache.ForeignKeyConstraint;
import net.sf.hajdbc.cache.TableProperties;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 * @author Paul Ferraro
 */
public class SybaseDialectTest extends StandardDialectTest
{
	public SybaseDialectTest()
	{
		super(DialectFactoryEnum.SYBASE);
	}
	
	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.dialect.StandardDialectTest#getIdentityColumnSupport()
	 */
	@Override
	public void getIdentityColumnSupport()
	{
		assertSame(this.dialect, this.dialect.getIdentityColumnSupport());
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.dialect.StandardDialectTest#getSimpleSQL()
	 */
	@Override
	public void getSimpleSQL() throws SQLException
	{
		assertEquals("SELECT GETDATE()", this.dialect.getSimpleSQL());
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.dialect.StandardDialectTest#getTruncateTableSQL()
	 */
	@Override
	public void getTruncateTableSQL() throws SQLException
	{
		TableProperties table = mock(TableProperties.class);
		
		when(table.getName()).thenReturn("table");
		
		String result = this.dialect.getTruncateTableSQL(table);
		
		assertEquals("TRUNCATE TABLE table", result);
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.dialect.StandardDialectTest#getCreateForeignKeyConstraintSQL()
	 */
	@Override
	public void getCreateForeignKeyConstraintSQL() throws SQLException
	{
		ForeignKeyConstraint key = mock(ForeignKeyConstraint.class);
		
		when(key.getName()).thenReturn("name");
		when(key.getTable()).thenReturn("table");
		when(key.getColumnList()).thenReturn(Arrays.asList("column1", "column2"));
		when(key.getForeignTable()).thenReturn("foreign_table");
		when(key.getForeignColumnList()).thenReturn(Arrays.asList("foreign_column1", "foreign_column2"));
		when(key.getDeferrability()).thenReturn(DatabaseMetaData.importedKeyInitiallyDeferred);
		when(key.getDeleteRule()).thenReturn(DatabaseMetaData.importedKeyCascade);
		when(key.getUpdateRule()).thenReturn(DatabaseMetaData.importedKeyRestrict);
		
		String result = this.dialect.getCreateForeignKeyConstraintSQL(key);
		
		assertEquals("ALTER TABLE table ADD CONSTRAINT name FOREIGN KEY (column1, column2) REFERENCES foreign_table (foreign_column1, foreign_column2) ON DELETE CASCADE ON UPDATE RESTRICT", result);
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.dialect.StandardDialectTest#evaluateCurrentDate()
	 */
	@Override
	public void evaluateCurrentDate()
	{
		java.sql.Date date = new java.sql.Date(System.currentTimeMillis());
		
		assertEquals(String.format("SELECT '%s' FROM test", date.toString()), this.dialect.evaluateCurrentDate("SELECT CURRENT DATE FROM test", date));
		assertEquals(String.format("SELECT '%s' FROM test", date.toString()), this.dialect.evaluateCurrentDate("SELECT TODAY(*) FROM test", date));
		assertEquals(String.format("SELECT '%s' FROM test", date.toString()), this.dialect.evaluateCurrentDate("SELECT TODAY ( * ) FROM test", date));
		assertEquals("SELECT CURRENT_DATE FROM test", this.dialect.evaluateCurrentDate("SELECT CURRENT_DATE FROM test", date));
		assertEquals("SELECT CURRENT DATES FROM test", this.dialect.evaluateCurrentDate("SELECT CURRENT DATES FROM test", date));
		assertEquals("SELECT SCURRENT DATE FROM test", this.dialect.evaluateCurrentDate("SELECT SCURRENT DATE FROM test", date));
		assertEquals("SELECT CURRENT_TIME FROM test", this.dialect.evaluateCurrentDate("SELECT CURRENT_TIME FROM test", date));
		assertEquals("SELECT CURRENT_TIMESTAMP FROM test", this.dialect.evaluateCurrentDate("SELECT CURRENT_TIMESTAMP FROM test", date));
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.dialect.StandardDialectTest#evaluateCurrentTime()
	 */
	@Override
	public void evaluateCurrentTime()
	{
		java.sql.Time time = new java.sql.Time(System.currentTimeMillis());
		
		assertEquals(String.format("SELECT '%s' FROM test", time.toString()), this.dialect.evaluateCurrentTime("SELECT CURRENT TIME FROM test", time));
		assertEquals("SELECT CURRENT_TIME FROM test", this.dialect.evaluateCurrentTime("SELECT CURRENT_TIME FROM test", time));
		assertEquals("SELECT LOCALTIME FROM test", this.dialect.evaluateCurrentTime("SELECT LOCALTIME FROM test", time));
		assertEquals("SELECT CURRENT_DATE FROM test", this.dialect.evaluateCurrentTime("SELECT CURRENT_DATE FROM test", time));
		assertEquals("SELECT CURRENT_TIMESTAMP FROM test", this.dialect.evaluateCurrentTime("SELECT CURRENT_TIMESTAMP FROM test", time));
		assertEquals("SELECT LOCALTIMESTAMP FROM test", this.dialect.evaluateCurrentTime("SELECT LOCALTIMESTAMP FROM test", time));
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.dialect.StandardDialectTest#evaluateCurrentTimestamp()
	 */
	@Override
	public void evaluateCurrentTimestamp()
	{
		java.sql.Timestamp timestamp = new java.sql.Timestamp(System.currentTimeMillis());
		
		assertEquals(String.format("SELECT '%s' FROM test", timestamp.toString()), this.dialect.evaluateCurrentTimestamp("SELECT CURRENT TIMESTAMP FROM test", timestamp));
		assertEquals(String.format("SELECT '%s' FROM test", timestamp.toString()), this.dialect.evaluateCurrentTimestamp("SELECT GETDATE() FROM test", timestamp));
		assertEquals(String.format("SELECT '%s' FROM test", timestamp.toString()), this.dialect.evaluateCurrentTimestamp("SELECT GETDATE ( ) FROM test", timestamp));
		assertEquals(String.format("SELECT '%s' FROM test", timestamp.toString()), this.dialect.evaluateCurrentTimestamp("SELECT NOW(*) FROM test", timestamp));
		assertEquals(String.format("SELECT '%s' FROM test", timestamp.toString()), this.dialect.evaluateCurrentTimestamp("SELECT NOW ( * ) FROM test", timestamp));
		assertEquals("SELECT CURRENT_TIMESTAMP FROM test", this.dialect.evaluateCurrentTimestamp("SELECT CURRENT_TIMESTAMP FROM test", timestamp));
		assertEquals("SELECT LOCALTIMESTAMP FROM test", this.dialect.evaluateCurrentTimestamp("SELECT LOCALTIMESTAMP FROM test", timestamp));
		assertEquals("SELECT CURRENT_DATE FROM test", this.dialect.evaluateCurrentTimestamp("SELECT CURRENT_DATE FROM test", timestamp));
		assertEquals("SELECT CURRENT_TIME FROM test", this.dialect.evaluateCurrentTimestamp("SELECT CURRENT_TIME FROM test", timestamp));
		assertEquals("SELECT LOCALTIME FROM test", this.dialect.evaluateCurrentTimestamp("SELECT LOCALTIME FROM test", timestamp));
	}
}
