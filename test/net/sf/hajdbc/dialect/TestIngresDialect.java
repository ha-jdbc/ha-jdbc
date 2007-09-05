/*
 * Copyright (c) 2004-2007, Identity Theft 911, LLC.  All rights reserved.
 */
package net.sf.hajdbc.dialect;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Iterator;

import net.sf.hajdbc.Dialect;

import org.easymock.EasyMock;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * @author Paul Ferraro
 */
@SuppressWarnings("nls")
public class TestIngresDialect extends TestStandardDialect
{
	/**
	 * @see net.sf.hajdbc.dialect.TestStandardDialect#createDialect()
	 */
	@Override
	protected Dialect createDialect()
	{
		return new IngresDialect();
	}

	/**
	 * @see net.sf.hajdbc.Dialect#parseInsertTable(java.lang.String)
	 */
	@Override
	@Test(dataProvider = "insert-table-sql")
	public String parseInsertTable(String sql) throws SQLException
	{
		this.replay();
		
		String table = this.dialect.parseInsertTable(sql);
		
		this.verify();

		assert table == null : table;
		
		return table;
	}

	/**
	 * @see net.sf.hajdbc.dialect.TestStandardDialect#getSequences(java.sql.Connection)
	 */
	@Override
	@Test(dataProvider = "connection")
	public Collection<String> getSequences(Connection connection) throws SQLException
	{
		EasyMock.expect(connection.createStatement()).andReturn(this.statement);
		EasyMock.expect(this.statement.executeQuery("SELECT seq_name FROM iisequence")).andReturn(this.resultSet);
		EasyMock.expect(this.resultSet.next()).andReturn(true);
		EasyMock.expect(this.resultSet.getString(1)).andReturn("sequence1");
		EasyMock.expect(this.resultSet.next()).andReturn(true);
		EasyMock.expect(this.resultSet.getString(1)).andReturn("sequence2");
		EasyMock.expect(this.resultSet.next()).andReturn(false);
		
		this.resultSet.close();
		this.statement.close();
		
		this.replay();
		
		Collection<String> sequences = this.dialect.getSequences(connection);
		
		this.verify();

		assert sequences.size() == 2 : sequences.size();
		
		Iterator<String> iterator = sequences.iterator();
		String sequence = iterator.next();
		
		assert sequence.equals("sequence1") : sequence;
		
		sequence = iterator.next();
		
		assert sequence.equals("sequence2") : sequence;
		
		return sequences;
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
			new Object[] { "SELECT 1 FROM failure" },
		};
	}
}
