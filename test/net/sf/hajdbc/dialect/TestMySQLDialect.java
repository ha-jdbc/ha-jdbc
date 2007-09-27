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
import java.util.List;

import net.sf.hajdbc.ColumnProperties;
import net.sf.hajdbc.Dialect;
import net.sf.hajdbc.ForeignKeyConstraint;
import net.sf.hajdbc.QualifiedName;
import net.sf.hajdbc.TableProperties;
import net.sf.hajdbc.UniqueConstraint;

import org.easymock.EasyMock;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * @author Paul Ferraro
 *
 */
@SuppressWarnings("nls")
public class TestMySQLDialect extends TestStandardDialect
{
	/**
	 * @see net.sf.hajdbc.dialect.TestStandardDialect#createDialect()
	 */
	@Override
	protected Dialect createDialect()
	{
		return new MySQLDialect();
	}

	/**
	 * @see net.sf.hajdbc.dialect.TestStandardDialect#getCreateForeignKeyConstraintSQL(net.sf.hajdbc.ForeignKeyConstraint)
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
	 * @see net.sf.hajdbc.dialect.TestStandardDialect#getCreateForeignKeyConstraintSQL(net.sf.hajdbc.ForeignKeyConstraint)
	 */
	@Override
	@Test(dataProvider = "foreign-key")
	public String getDropForeignKeyConstraintSQL(ForeignKeyConstraint constraint) throws SQLException
	{
		this.replay();
		
		String sql = this.dialect.getDropForeignKeyConstraintSQL(constraint);
		
		this.verify();
		
		assert sql.equals("ALTER TABLE table DROP FOREIGN KEY name") : sql;
		
		return sql;
	}

	/**
	 * @see net.sf.hajdbc.dialect.TestStandardDialect#getCreateForeignKeyConstraintSQL(net.sf.hajdbc.ForeignKeyConstraint)
	 */
	@Override
	@Test(dataProvider = "unique-constraint")
	public String getCreateUniqueConstraintSQL(UniqueConstraint constraint) throws SQLException
	{
		this.replay();
		
		String sql = this.dialect.getCreateUniqueConstraintSQL(constraint);
		
		this.verify();
		
		assert sql.equals("ALTER TABLE table ADD UNIQUE name (column1, column2)") : sql;
		
		return sql;
	}

	/**
	 * @see net.sf.hajdbc.dialect.TestStandardDialect#getCreateForeignKeyConstraintSQL(net.sf.hajdbc.ForeignKeyConstraint)
	 */
	@Override
	@Test(dataProvider = "unique-constraint")
	public String getDropUniqueConstraintSQL(UniqueConstraint constraint) throws SQLException
	{
		this.replay();
		
		String sql = this.dialect.getDropUniqueConstraintSQL(constraint);
		
		this.verify();
		
		assert sql.equals("ALTER TABLE table DROP INDEX name") : sql;
		
		return sql;
	}

	/**
	 * @see net.sf.hajdbc.dialect.TestStandardDialect#parseSequence(java.lang.String)
	 */
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

	/**
	 * @see net.sf.hajdbc.dialect.TestStandardDialect#getSequences(java.sql.Connection)
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

	/**
	 * @see net.sf.hajdbc.dialect.TestStandardDialect#getDefaultSchemas(java.sql.Connection)
	 */
	@Override
	@Test(dataProvider = "meta-data")
	public List<String> getDefaultSchemas(DatabaseMetaData metaData) throws SQLException
	{
		EasyMock.expect(metaData.getConnection()).andReturn(this.connection);
		EasyMock.expect(this.connection.createStatement()).andReturn(this.statement);
		EasyMock.expect(this.statement.executeQuery("SELECT DATABASE()")).andReturn(this.resultSet);
		EasyMock.expect(this.resultSet.next()).andReturn(false);
		EasyMock.expect(this.resultSet.getString(1)).andReturn("database");

		this.resultSet.close();
		this.statement.close();
		
		this.replay();
		
		List<String> schemaList = this.dialect.getDefaultSchemas(metaData);
		
		this.verify();
		
		assert schemaList.size() == 1 : schemaList.size();
		
		assert schemaList.get(0).equals("database") : schemaList.get(0);
		
		return schemaList;
	}

	/**
	 * @see net.sf.hajdbc.dialect.TestStandardDialect#isIdentity(net.sf.hajdbc.ColumnProperties)
	 */
	@Override
	@Test(dataProvider = "column")
	public boolean isIdentity(ColumnProperties properties) throws SQLException
	{
		EasyMock.expect(properties.getNativeType()).andReturn("SERIAL");
		
		this.replay();
		
		boolean identity = this.dialect.isIdentity(properties);
		
		this.verify();

		assert identity;
		
		this.reset();
		
		EasyMock.expect(properties.getNativeType()).andReturn("INTEGER");
		EasyMock.expect(properties.getRemarks()).andReturn("AUTO_INCREMENT");
		
		this.replay();
		
		identity = this.dialect.isIdentity(properties);
		
		this.verify();
		
		assert identity;

		this.reset();
		
		EasyMock.expect(this.columnProperties.getNativeType()).andReturn("INTEGER");
		EasyMock.expect(this.columnProperties.getRemarks()).andReturn(null);
		
		this.replay();
		
		identity = this.dialect.isIdentity(properties);
		
		this.verify();
		
		return identity;
	}
	
	/**
	 * @see net.sf.hajdbc.Dialect#getAlterIdentityColumnSQL(net.sf.hajdbc.TableProperties, net.sf.hajdbc.ColumnProperties, long)
	 */
	@Override
	@Test(dataProvider = "table-column-long")
	public String getAlterIdentityColumnSQL(TableProperties table, ColumnProperties column, long value) throws SQLException
	{
		EasyMock.expect(table.getName()).andReturn("table");
		EasyMock.expect(column.getName()).andReturn("column");
		
		this.replay();
		
		String sql = this.dialect.getAlterIdentityColumnSQL(table, column, value);
		
		this.verify();
		
		assert sql.equals("ALTER TABLE table AUTO_INCREMENT = 1") : sql;
		
		return sql;
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
	public String evaluateCurrentTimestamp(String sql, java.sql.Timestamp date)
	{
		String expected = sql.contains("success") ? "SELECT '" + date.toString() + "' FROM success" : sql;
		
		String evaluated = this.dialect.evaluateCurrentTimestamp(sql, date);

		assert evaluated.equals(expected) : evaluated;
		
		return evaluated;
	}
}
