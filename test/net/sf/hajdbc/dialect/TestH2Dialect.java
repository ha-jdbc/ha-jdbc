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
import java.util.Iterator;

import net.sf.hajdbc.ForeignKeyConstraint;
import net.sf.hajdbc.QualifiedName;
import net.sf.hajdbc.SequenceProperties;
import net.sf.hajdbc.cache.ForeignKeyConstraintImpl;

import org.easymock.EasyMock;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * @author Paul Ferraro
 */
@SuppressWarnings("nls")
@Test
public class TestH2Dialect extends TestStandardDialect
{
	public TestH2Dialect()
	{
		super(new H2Dialect());
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
	 * @see net.sf.hajdbc.dialect.TestStandardDialect#testGetSequences()
	 */
	@Override
	public void testGetSequences() throws SQLException
	{
		DatabaseMetaData metaData = EasyMock.createStrictMock(DatabaseMetaData.class);
		Connection connection = EasyMock.createStrictMock(Connection.class);
		Statement statement = EasyMock.createStrictMock(Statement.class);
		ResultSet resultSet = EasyMock.createStrictMock(ResultSet.class);
		
		EasyMock.expect(metaData.getConnection()).andReturn(connection);
		EasyMock.expect(connection.createStatement()).andReturn(statement);
		EasyMock.expect(statement.executeQuery("SELECT SEQUENCE_SCHEMA, SEQUENCE_NAME FROM INFORMATION_SCHEMA.SYSTEM_SEQUENCES")).andReturn(resultSet);
		EasyMock.expect(resultSet.next()).andReturn(true);
		EasyMock.expect(resultSet.getString(1)).andReturn("schema1");
		EasyMock.expect(resultSet.getString(2)).andReturn("sequence1");
		EasyMock.expect(resultSet.next()).andReturn(true);
		EasyMock.expect(resultSet.getString(1)).andReturn("schema2");
		EasyMock.expect(resultSet.getString(2)).andReturn("sequence2");
		EasyMock.expect(resultSet.next()).andReturn(false);
		
		statement.close();
	
		EasyMock.replay(metaData, connection, statement, resultSet);
		
		Collection<QualifiedName> result = this.getSequences(metaData);
		
		EasyMock.verify(metaData, connection, statement, resultSet);
		
		assert result.size() == 2 : result;
		
		Iterator<QualifiedName> iterator = result.iterator();
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

	/**
	 * @see net.sf.hajdbc.dialect.TestStandardDialect#testGetSimpleSQL()
	 */
	@Override
	public void testGetSimpleSQL() throws SQLException
	{
		String result = this.getSimpleSQL();
		
		assert result.equals("CALL CURRENT_TIMESTAMP") : result;
	}
	
	/**
	 * @see net.sf.hajdbc.dialect.TestStandardDialect#testGetNextSequenceValueSQL()
	 */
	@Override
	public void testGetNextSequenceValueSQL() throws SQLException
	{
		SequenceProperties sequence = EasyMock.createStrictMock(SequenceProperties.class);
		
		EasyMock.expect(sequence.getName()).andReturn("sequence");
		
		EasyMock.replay(sequence);
		
		String result = this.getNextSequenceValueSQL(sequence);

		EasyMock.verify(sequence);
		
		assert result.equals("CALL NEXT VALUE FOR sequence") : result;
	}
	
	@Override
	@DataProvider(name = "current-date")
	Object[][] currentDateProvider()
	{
		java.sql.Date date = new java.sql.Date(System.currentTimeMillis());
		
		return new Object[][] {
			new Object[] { "SELECT CURRENT_DATE FROM success", date },
			new Object[] { "SELECT CURRENT_DATE() FROM success", date },
			new Object[] { "SELECT CURRENT_DATE ( ) FROM success", date },
			new Object[] { "SELECT CURDATE() FROM success", date },
			new Object[] { "SELECT CURDATE ( ) FROM success", date },
			new Object[] { "SELECT CURRENT_DATES FROM failure", date },
			new Object[] { "SELECT CCURRENT_DATE FROM failure", date },
			new Object[] { "SELECT CCURDATE() FROM failure", date },
			new Object[] { "SELECT 1 FROM failure", date },
		};
	}

	@Override
	@DataProvider(name = "current-time")
	Object[][] currentTimeProvider()
	{
		java.sql.Time date = new java.sql.Time(System.currentTimeMillis());
		
		return new Object[][] {
			new Object[] { "SELECT CURRENT_TIME FROM success", date },
			new Object[] { "SELECT CURRENT_TIME() FROM success", date },
			new Object[] { "SELECT CURRENT_TIME ( ) FROM success", date },
			new Object[] { "SELECT CURTIME() FROM success", date },
			new Object[] { "SELECT CURTIME ( ) FROM success", date },
			new Object[] { "SELECT CCURRENT_TIME FROM failure", date },
			new Object[] { "SELECT CURRENT_TIMESTAMP FROM failure", date },
			new Object[] { "SELECT CCURTIME() FROM failure", date },
			new Object[] { "SELECT 1 FROM failure", date },
		};
	}

	@Override
	@DataProvider(name = "current-timestamp")
	Object[][] currentTimestampProvider()
	{
		java.sql.Timestamp date = new java.sql.Timestamp(System.currentTimeMillis());
		
		return new Object[][] {
			new Object[] { "SELECT CURRENT_TIMESTAMP FROM success", date },
			new Object[] { "SELECT CURRENT_TIMESTAMP() FROM success", date },
			new Object[] { "SELECT CURRENT_TIMESTAMP ( ) FROM success", date },
			new Object[] { "SELECT NOW() FROM success", date },
			new Object[] { "SELECT NOW ( ) FROM success", date },
			new Object[] { "SELECT CURRENT_TIMESTAMPS FROM failure", date },
			new Object[] { "SELECT CCURRENT_TIMESTAMP FROM failure", date },
			new Object[] { "SELECT NNOW() FROM failure", date },
			new Object[] { "SELECT 1 FROM failure", date },
		};
	}
}
