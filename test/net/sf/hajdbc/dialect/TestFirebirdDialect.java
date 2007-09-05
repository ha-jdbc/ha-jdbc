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
import java.sql.SQLException;
import java.util.Collection;
import java.util.Iterator;

import net.sf.hajdbc.Dialect;

import org.easymock.EasyMock;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * @author Paul Ferraro
 *
 */
@SuppressWarnings("nls")
public class TestFirebirdDialect extends TestStandardDialect
{
	/**
	 * @see net.sf.hajdbc.dialect.TestStandardDialect#createDialect()
	 */
	@Override
	protected Dialect createDialect()
	{
		return new FirebirdDialect();
	}
	
	/**
	 * @see net.sf.hajdbc.dialect.TestStandardDialect#getAlterSequenceSQL(java.lang.String, long)
	 */
	@Override
	@Test(dataProvider = "alter-sequence")
	public String getAlterSequenceSQL(String sequence, long value) throws SQLException
	{
		this.replay();
		
		String sql = this.dialect.getAlterSequenceSQL(sequence, value);
		
		this.verify();
		
		assert sql.equals("SET GENERATOR sequence TO 1") : sql;
		
		return sql;
	}

	/**
	 * @see net.sf.hajdbc.dialect.TestStandardDialect#getSequences(java.sql.Connection)
	 */
	@Override
	@Test(dataProvider = "connection")
	public Collection<String> getSequences(Connection connection) throws SQLException
	{
		EasyMock.expect(connection.createStatement()).andReturn(this.statement);
		EasyMock.expect(this.statement.executeQuery("SELECT RDB$GENERATOR_NAME FROM RDB$GENERATORS")).andReturn(this.resultSet);
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
		
		assert sequences.size() == 2 : sequences;
		
		Iterator<String> iterator = sequences.iterator();
		String sequence = iterator.next();
		
		assert sequence.equals("sequence1") : sequence;

		sequence = iterator.next();
		
		assert sequence.equals("sequence2") : sequence;
		
		return sequences;
	}
	
	/**
	 * @see net.sf.hajdbc.Dialect#getCurrentSequenceValueSQL(java.lang.String)
	 */
	@Override
	@Test(dataProvider = "sequence")
	public String getNextSequenceValueSQL(String sequence) throws SQLException
	{
		this.replay();
		
		String sql = this.dialect.getNextSequenceValueSQL(sequence);
		
		this.verify();
		
		assert sql.equals("SELECT GEN_ID(sequence, 1) FROM RDB$DATABASE") : sql;
		
		return sql;
	}

	/**
	 * @see net.sf.hajdbc.dialect.TestStandardDialect#getSimpleSQL()
	 */
	@Override
	@Test
	public String getSimpleSQL() throws SQLException
	{
		String sql = this.dialect.getSimpleSQL();
		
		assert sql.equals("SELECT CURRENT_TIMESTAMP FROM RDB$DATABASE") : sql;
		
		return sql;
	}

	/**
	 * @see net.sf.hajdbc.dialect.TestStandardDialect#isSelectForUpdate(java.lang.String)
	 */
	@Override
	@Test(dataProvider = "null")
	public boolean isSelectForUpdate(String sql) throws SQLException
	{
		this.replay();
		
		boolean selectForUpdate = this.dialect.isSelectForUpdate("SELECT * FROM table FOR UPDATE");
		
		this.verify();
		
		assert !selectForUpdate;
		
		this.replay();
		
		selectForUpdate = this.dialect.isSelectForUpdate("SELECT * FROM table FOR UPDATE WITH LOCK");
		
		this.verify();
		
		assert selectForUpdate;
		
		return selectForUpdate;
	}

	@Override
	@DataProvider(name = "sequence-sql")
	Object[][] sequenceSQLProvider()
	{
		return new Object[][] {
			new Object[] { "SELECT GEN_ID(success, 1) FROM RDB$DATABASE" },
			new Object[] { "SELECT GEN_ID(success, 1), * FROM table" },
			new Object[] { "INSERT INTO table VALUES (GEN_ID(success, 1), 0)" },
			new Object[] { "UPDATE table SET id = GEN_ID(success, 1)" },
			new Object[] { "SELECT * FROM table" },
		};
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

		assert (table == null) : table;

		return table;
	}
}
