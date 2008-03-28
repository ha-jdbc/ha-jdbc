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
import net.sf.hajdbc.Dialect;
import net.sf.hajdbc.ForeignKeyConstraint;
import net.sf.hajdbc.QualifiedName;
import net.sf.hajdbc.SequenceProperties;

import org.easymock.EasyMock;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * @author Paul Ferraro
 *
 */
@SuppressWarnings("nls")
public class TestDerbyDialect extends TestStandardDialect
{
	@Override
	protected Dialect createDialect()
	{
		return new DerbyDialect();
	}

	@Override
	@Test(dataProvider = "foreign-key")
	public String getCreateForeignKeyConstraintSQL(ForeignKeyConstraint constraint) throws SQLException
	{
		this.replay();
		
		String sql = this.dialect.getCreateForeignKeyConstraintSQL(constraint);
		
		this.verify();
		
		assert sql.equals("ALTER TABLE table ADD CONSTRAINT name FOREIGN KEY (column1, column2) REFERENCES foreign_table (foreign_column1, foreign_column2) ON DELETE CASCADE ON UPDATE RESTRICT") : sql;
		
		return sql;
	}

	@Override
	@Test
	public String getSimpleSQL() throws SQLException
	{
		this.replay();
		
		String sql = this.dialect.getSimpleSQL();
		
		this.verify();
		
		assert sql.equals("VALUES CURRENT_TIMESTAMP") : sql;
		
		return sql;
	}

	@Override
	@Test(dataProvider = "sequence-sql")
	public String parseSequence(String sql) throws SQLException
	{
		this.replay();
		
		String sequence = this.dialect.parseSequence(sql);
		
		this.verify();
		
		assert sequence == null : sequence;
		
		return sequence;
	}

	@Override
	@Test(dataProvider = "meta-data")
	public Collection<QualifiedName> getSequences(DatabaseMetaData metaData) throws SQLException
	{
		this.replay();
		
		Collection<QualifiedName> sequences = this.dialect.getSequences(metaData);
		
		this.verify();
		
		assert sequences.isEmpty() : sequences;
		
		return sequences;
	}

	@Override
	@Test(dataProvider = "column")
	public boolean isIdentity(ColumnProperties properties) throws SQLException
	{
		EasyMock.expect(properties.getRemarks()).andReturn("GENERATED ALWAYS AS IDENTITY");
		
		this.replay();
		
		boolean identity = this.dialect.isIdentity(properties);
		
		this.verify();
		
		assert identity;
		
		this.reset();
		
		EasyMock.expect(this.columnProperties.getRemarks()).andReturn(null);
		
		this.replay();
		
		identity = this.dialect.isIdentity(properties);
		
		this.verify();

		assert !identity;
		
		return identity;
	}
	
	@Override
	@Test(dataProvider = "sequence")
	public String getNextSequenceValueSQL(SequenceProperties sequence) throws SQLException
	{
		EasyMock.expect(sequence.getName()).andReturn("sequence");
		
		this.replay();
		
		String sql = this.dialect.getNextSequenceValueSQL(sequence);
		
		this.verify();
		
		assert sql.equals("VALUES NEXT VALUE FOR sequence") : sql;
		
		return sql;
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
	public String evaluateCurrentDate(String sql, java.sql.Date date) throws SQLException
	{
		String expected = sql.contains("success") ? "SELECT DATE('" + date.toString() + "') FROM success" : sql;
		
		String evaluated = this.dialect.evaluateCurrentDate(sql, date);

		assert evaluated.equals(expected) : evaluated;
		
		return evaluated;
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
	public String evaluateCurrentTime(String sql, java.sql.Time date) throws SQLException
	{
		String expected = sql.contains("success") ? "SELECT TIME('" + date.toString() + "') FROM success" : sql;
		
		String evaluated = this.dialect.evaluateCurrentTime(sql, date);

		assert evaluated.equals(expected) : evaluated;
		
		return evaluated;
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
	public String evaluateCurrentTimestamp(String sql, java.sql.Timestamp date) throws SQLException
	{
		String expected = sql.contains("success") ? "SELECT TIMESTAMP('" + date.toString() + "') FROM success" : sql;
		
		String evaluated = this.dialect.evaluateCurrentTimestamp(sql, date);

		assert evaluated.equals(expected) : evaluated;
		
		return evaluated;
	}
}
