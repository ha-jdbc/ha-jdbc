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
import java.util.Collection;

import net.sf.hajdbc.ColumnProperties;
import net.sf.hajdbc.ForeignKeyConstraint;
import net.sf.hajdbc.QualifiedName;
import net.sf.hajdbc.SequenceProperties;
import net.sf.hajdbc.cache.ForeignKeyConstraintImpl;

import org.easymock.EasyMock;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * @author Paul Ferraro
 *
 */
@SuppressWarnings("nls")
@Test
public class TestDerbyDialect extends TestStandardDialect
{
	public TestDerbyDialect()
	{
		super(new DerbyDialect());
	}

	/**
	 * @see net.sf.hajdbc.dialect.TestStandardDialect#testGetCreateForeignKeyConstraintSQL()
	 */
	@Override
	public void testGetCreateForeignKeyConstraintSQL()
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
	 * @see net.sf.hajdbc.dialect.TestStandardDialect#testGetSimpleSQL()
	 */
	@Override
	public void testGetSimpleSQL()
	{
		String result = this.getSimpleSQL();
		
		assert result.equals("VALUES CURRENT_TIMESTAMP") : result;
	}

	/**
	 * @see net.sf.hajdbc.dialect.TestStandardDialect#testParseSequence(java.lang.String)
	 */
	@Override
	@Test(dataProvider = "sequence-sql")
	public void testParseSequence(String sql)
	{
		String result = this.parseSequence(sql);
		
		assert result == null : result;
	}

	/**
	 * @see net.sf.hajdbc.dialect.TestStandardDialect#testGetSequences()
	 */
	@Override
	public void testGetSequences()
	{
		DatabaseMetaData metaData = EasyMock.createStrictMock(DatabaseMetaData.class);
		
		EasyMock.replay(metaData);
		
		Collection<QualifiedName> result = this.getSequences(metaData);

		EasyMock.verify(metaData);
		
		assert result.isEmpty() : result;
	}

	/**
	 * @see net.sf.hajdbc.dialect.TestStandardDialect#testIsIdentity()
	 */
	@Override
	public void testIsIdentity()
	{
		ColumnProperties column = EasyMock.createStrictMock(ColumnProperties.class);
		
		EasyMock.expect(column.getRemarks()).andReturn("GENERATED ALWAYS AS IDENTITY");
		
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
	
	/**
	 * @see net.sf.hajdbc.dialect.TestStandardDialect#testGetNextSequenceValueSQL()
	 */
	@Override
	public void testGetNextSequenceValueSQL()
	{
		SequenceProperties sequence = EasyMock.createStrictMock(SequenceProperties.class);
		
		EasyMock.expect(sequence.getName()).andReturn("sequence");
		
		EasyMock.replay(sequence);

		String result = this.getNextSequenceValueSQL(sequence);
		
		EasyMock.verify(sequence);
		
		assert result.equals("VALUES NEXT VALUE FOR sequence") : result;
	}

	@Override
	@DataProvider(name = "current-date")
	Object[][] currentDateProvider()
	{
		java.sql.Date date = new java.sql.Date(System.currentTimeMillis());
		
		return new Object[][] {
			new Object[] { "SELECT CURRENT_DATE FROM success", date },
			new Object[] { "SELECT CURRENT DATE FROM success", date },
			new Object[] { "SELECT CCURRENT_DATE FROM failure", date },
			new Object[] { "SELECT CURRENT_DATES FROM failure", date },
			new Object[] { "SELECT 1 FROM failure", date },
		};
	}
	
	@Override
	@Test(dataProvider = "current-date")
	public void testEvaluateCurrentDate(String sql, java.sql.Date date)
	{
		String expected = sql.contains("success") ? String.format("SELECT DATE('%s') FROM success", date.toString()) : sql;
		
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
			new Object[] { "SELECT CURRENT TIME FROM success", date },
			new Object[] { "SELECT CCURRENT_TIME FROM failure", date },
			new Object[] { "SELECT CURRENT_TIMESTAMP FROM failure", date },
			new Object[] { "SELECT 1 FROM failure", date },
		};
	}
	
	@Override
	@Test(dataProvider = "current-time")
	public void testEvaluateCurrentTime(String sql, java.sql.Time date)
	{
		String expected = sql.contains("success") ? String.format("SELECT TIME('%s') FROM success", date.toString()) : sql;
		
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
			new Object[] { "SELECT CURRENT TIMESTAMP FROM success", date },
			new Object[] { "SELECT CCURRENT_TIMESTAMP FROM failure", date },
			new Object[] { "SELECT CURRENT_TIMESTAMPS FROM failure", date },
			new Object[] { "SELECT 1 FROM failure", date },
		};
	}
	
	@Override
	@Test(dataProvider = "current-timestamp")
	public void testEvaluateCurrentTimestamp(String sql, java.sql.Timestamp date)
	{
		String expected = sql.contains("success") ? String.format("SELECT TIMESTAMP('%s') FROM success", date.toString()) : sql;
		
		String evaluated = this.evaluateCurrentTimestamp(sql, date);

		assert evaluated.equals(expected) : evaluated;
	}
}
