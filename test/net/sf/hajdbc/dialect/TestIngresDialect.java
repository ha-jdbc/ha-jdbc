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

import net.sf.hajdbc.QualifiedName;

import org.easymock.EasyMock;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * @author Paul Ferraro
 */
@SuppressWarnings("nls")
@Test
public class TestIngresDialect extends TestStandardDialect
{
	public TestIngresDialect()
	{
		super(new IngresDialect());
	}

	/**
	 * @see net.sf.hajdbc.dialect.TestStandardDialect#testParseInsertTable(java.lang.String)
	 */
	@Override
	@Test(dataProvider = "insert-table-sql")
	public void testParseInsertTable(String sql) throws SQLException
	{
		String result = this.parseInsertTable(sql);
		
		assert (result == null) : result;
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
		EasyMock.expect(statement.executeQuery("SELECT seq_name FROM iisequence")).andReturn(resultSet);
		EasyMock.expect(resultSet.next()).andReturn(true);
		EasyMock.expect(resultSet.getString(1)).andReturn("sequence1");
		EasyMock.expect(resultSet.next()).andReturn(true);
		EasyMock.expect(resultSet.getString(1)).andReturn("sequence2");
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
		
		assert (schema == null) : schema;
		assert name.equals("sequence1") : name;
		
		sequence = iterator.next();
		schema = sequence.getSchema();
		name = sequence.getName();
		
		assert (schema == null) : schema;
		assert name.equals("sequence2") : name;
	}

	@Override
	@DataProvider(name = "sequence-sql")
	Object[][] sequenceSQLProvider()
	{
		return new Object[][] {
			new Object[] { "SELECT NEXT VALUE FOR success" },
			new Object[] { "SELECT CURRENT VALUE FOR success" },
			new Object[] { "SELECT NEXT VALUE FOR success, * FROM table" },
			new Object[] { "SELECT CURRENT VALUE FOR success, * FROM table" },
			new Object[] { "INSERT INTO table VALUES (NEXT VALUE FOR success, 0)" },
			new Object[] { "INSERT INTO table VALUES (CURRENT VALUE FOR success, 0)" },
			new Object[] { "UPDATE table SET id = NEXT VALUE FOR success" },
			new Object[] { "UPDATE table SET id = CURRENT VALUE FOR success" },
			new Object[] { "SELECT success.nextval" },
			new Object[] { "SELECT success.currval" },
			new Object[] { "SELECT success.nextval, * FROM table" },
			new Object[] { "SELECT success.currval, * FROM table" },
			new Object[] { "INSERT INTO table VALUES (success.nextval, 0)" },
			new Object[] { "INSERT INTO table VALUES (success.currval, 0)" },
			new Object[] { "UPDATE table SET id = success.nextval" },
			new Object[] { "UPDATE table SET id = success.currval" },
			new Object[] { "SELECT * FROM table" },
		};
	}

	@Override
	@DataProvider(name = "current-date")
	Object[][] currentDateProvider()
	{
		java.sql.Date date = new java.sql.Date(System.currentTimeMillis());
		
		return new Object[][] {
			new Object[] { "SELECT CURRENT_DATE FROM success", date },
			new Object[] { "SELECT DATE('TODAY') FROM success", date },
			new Object[] { "SELECT DATE ( 'TODAY' ) FROM success", date },
			new Object[] { "SELECT CURRENT_DATES FROM failure", date },
			new Object[] { "SELECT CCURRENT_DATE FROM failure", date },
			new Object[] { "SELECT DDATE('TODAY') FROM failure", date },
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
			new Object[] { "SELECT LOCAL_TIME FROM success", date },
			new Object[] { "SELECT CURRENT_TIMESTAMP FROM failure", date },
			new Object[] { "SELECT CCURRENT_TIME FROM failure", date },
			new Object[] { "SELECT LOCAL_TIMESTAMP FROM failure", date },
			new Object[] { "SELECT LLOCAL_TIME FROM failure", date },
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
			new Object[] { "SELECT LOCAL_TIMESTAMP FROM success", date },
			new Object[] { "SELECT DATE('NOW') FROM success", date },
			new Object[] { "SELECT DATE( 'NOW' ) FROM success", date },
			new Object[] { "SELECT CURRENT_TIMESTAMPS FROM failure", date },
			new Object[] { "SELECT CCURRENT_TIMESTAMP FROM failure", date },
			new Object[] { "SELECT LOCAL_TIMESTAMPS FROM failure", date },
			new Object[] { "SELECT LLOCAL_TIMESTAMP FROM failure", date },
			new Object[] { "SELECT DDATE('NOW') FROM failure", date },
			new Object[] { "SELECT 1 FROM failure", date },
		};
	}

	@Override
	@DataProvider(name = "random")
	Object[][] randomProvider()
	{
		return new Object[][] {
			new Object[] { "SELECT RANDOMF() FROM success" },
			new Object[] { "SELECT RANDOMF ( ) FROM success" },
			new Object[] { "SELECT OPERANDOMF( ) FROM failure" },
			new Object[] { "SELECT RAND() FROM failure" },
			new Object[] { "SELECT 1 FROM failure" },
		};
	}
}
