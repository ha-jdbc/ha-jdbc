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
import net.sf.hajdbc.TableProperties;

import org.easymock.EasyMock;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * @author Paul Ferraro
 */
@SuppressWarnings("nls")
public class TestSybaseDialect extends TestStandardDialect
{
	/**
	 * @see net.sf.hajdbc.dialect.TestStandardDialect#createDialect()
	 */
	@Override
	protected Dialect createDialect()
	{
		return new SybaseDialect();
	}

	/**
	 * @throws SQLException 
	 * @see net.sf.hajdbc.Dialect#getLockTableSQL(net.sf.hajdbc.TableProperties)
	 */
	@Override
	@Test(dataProvider = "table")
	public String getLockTableSQL(TableProperties properties) throws SQLException
	{
		EasyMock.expect(properties.getName()).andReturn("table");
		
		this.replay();
		
		String sql = this.dialect.getLockTableSQL(properties);
		
		this.verify();
		
		assert sql.equals("LOCK TABLE table IN SHARE MODE") : sql;
		
		return sql;
	}

	/**
	 * @see net.sf.hajdbc.Dialect#getTruncateTableSQL(net.sf.hajdbc.TableProperties)
	 */
	@Override
	@Test(dataProvider = "table")
	public String getTruncateTableSQL(TableProperties properties) throws SQLException
	{
		EasyMock.expect(properties.getName()).andReturn("table");
		
		this.replay();
		
		String sql = this.dialect.getTruncateTableSQL(properties);
		
		this.verify();
		
		assert sql.equals("TRUNCATE TABLE table");
		
		return sql;
	}

	/**
	 * @see net.sf.hajdbc.Dialect#getCreateForeignKeyConstraintSQL(net.sf.hajdbc.ForeignKeyConstraint)
	 */
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

	/**
	 * @see net.sf.hajdbc.Dialect#isIdentity(net.sf.hajdbc.ColumnProperties)
	 */
	@Override
	@Test(dataProvider = "column")
	public boolean isIdentity(ColumnProperties properties) throws SQLException
	{
		EasyMock.expect(properties.getDefaultValue()).andReturn("AUTOINCREMENT");
		
		this.replay();
		
		boolean identity = this.dialect.isIdentity(properties);
		
		this.verify();
		
		assert identity;
		
		this.reset();
		
		EasyMock.expect(properties.getDefaultValue()).andReturn("IDENTITY");
		
		this.replay();
		
		identity = this.dialect.isIdentity(properties);
		
		this.verify();
		
		assert identity;
		
		this.reset();
		
		EasyMock.expect(this.columnProperties.getDefaultValue()).andReturn(null);
		
		this.replay();
		
		identity = this.dialect.isIdentity(properties);
		
		this.verify();

		assert !identity;
		
		return identity;
	}
	
	/**
	 * @see net.sf.hajdbc.Dialect#parseSequence(java.lang.String)
	 */
	@Override
	@Test(dataProvider = "sequence-sql")
	public String parseSequence(String sql) throws SQLException
	{
		this.replay();
		
		String sequence = this.dialect.parseSequence(sql);
		
		this.verify();
		
		assert (sequence == null) : sequence;
		
		return sequence;
	}

	/**
	 * @see net.sf.hajdbc.Dialect#getSequences(java.sql.Connection)
	 */
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
	@DataProvider(name = "current-date")
	Object[][] currentDateProvider()
	{
		java.sql.Date date = new java.sql.Date(System.currentTimeMillis());
		
		return new Object[][] {
			new Object[] { "SELECT CURRENT DATE FROM success", date },
			new Object[] { "SELECT TODAY(*) FROM success", date },
			new Object[] { "SELECT TODAY ( * ) FROM success", date },
			new Object[] { "SELECT CURRENT DATES FROM failure", date },
			new Object[] { "SELECT CCURRENT DATE FROM failure", date },
			new Object[] { "SELECT NOTTODAY(*) FROM failure", date },
			new Object[] { "SELECT 1 FROM failure", date },
		};
	}
	
	@Override
	@Test(dataProvider = "current-date")
	public String evaluateCurrentDate(String sql, java.sql.Date date)
	{
		String expected = sql.contains("success") ? "SELECT '" + date.toString() + "' FROM success" : sql;
		
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
			new Object[] { "SELECT CURRENT TIME FROM success", date },
			new Object[] { "SELECT CCURRENT TIME FROM failure", date },
			new Object[] { "SELECT CURRENT TIMESTAMP FROM failure", date },
			new Object[] { "SELECT 1 FROM failure", date },
		};
	}
	
	@Override
	@Test(dataProvider = "current-time")
	public String evaluateCurrentTime(String sql, java.sql.Time date)
	{
		String expected = sql.contains("success") ? "SELECT '" + date.toString() + "' FROM success" : sql;
		
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
			new Object[] { "SELECT CURRENT TIMESTAMP FROM success", date },
			new Object[] { "SELECT GETDATE() FROM success", date },
			new Object[] { "SELECT GETDATE ( ) FROM success", date },
			new Object[] { "SELECT NOW(*) FROM success", date },
			new Object[] { "SELECT NOW ( * ) FROM success", date },
			new Object[] { "SELECT CCURRENT TIMESTAMP FROM failure", date },
			new Object[] { "SELECT CURRENT TIMESTAMPS FROM failure", date },
			new Object[] { "SELECT FORGETDATE() FROM failure", date },
			new Object[] { "SELECT NNOW(*) FROM failure", date },
			new Object[] { "SELECT 1 FROM failure", date },
		};
	}
	
	@Override
	@Test(dataProvider = "current-timestamp")
	public String evaluateCurrentTimestamp(String sql, java.sql.Timestamp date)
	{
		String expected = sql.contains("success") ? "SELECT '" + date.toString() + "' FROM success" : sql;
		
		String evaluated = this.dialect.evaluateCurrentTimestamp(sql, date);

		assert evaluated.equals(expected) : evaluated;
		
		return evaluated;
	}
}
