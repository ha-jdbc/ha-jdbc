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
import net.sf.hajdbc.SequenceProperties;

import org.easymock.EasyMock;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * @author Paul Ferraro
 *
 */
@SuppressWarnings("nls")
@Test
public class TestFirebirdDialect extends TestStandardDialect
{
	public TestFirebirdDialect()
	{
		super(new FirebirdDialect());
	}
	
	@Override
	public void testGetAlterSequenceSQL()
	{
		SequenceProperties sequence = EasyMock.createStrictMock(SequenceProperties.class);
		
		EasyMock.expect(sequence.getName()).andReturn("sequence");
		
		EasyMock.replay(sequence);
		
		String result = this.getAlterSequenceSQL(sequence, 1000L);
		
		EasyMock.verify(sequence);
		
		assert result.equals("SET GENERATOR sequence TO 1000") : result;
	}

	/**
	 * @see net.sf.hajdbc.dialect.TestStandardDialect#testGetSequences()
	 */
	@Override
	public void testGetSequences()
	{
		DatabaseMetaData metaData = EasyMock.createStrictMock(DatabaseMetaData.class);
		Connection connection = EasyMock.createStrictMock(Connection.class);
		Statement statement = EasyMock.createStrictMock(Statement.class);
		ResultSet resultSet = EasyMock.createStrictMock(ResultSet.class);
		
		try
		{
			EasyMock.expect(metaData.getConnection()).andReturn(connection);
			EasyMock.expect(connection.createStatement()).andReturn(statement);
			EasyMock.expect(statement.executeQuery("SELECT RDB$GENERATOR_NAME FROM RDB$GENERATORS")).andReturn(resultSet);
			EasyMock.expect(resultSet.next()).andReturn(true);
			EasyMock.expect(resultSet.getString(1)).andReturn("sequence1");
			EasyMock.expect(resultSet.next()).andReturn(true);
			EasyMock.expect(resultSet.getString(1)).andReturn("sequence2");
			EasyMock.expect(resultSet.next()).andReturn(false);
			
			statement.close();
			
			EasyMock.replay(metaData, connection, statement, resultSet);
			
			Collection<QualifiedName> results = this.getSequences(metaData);
			
			EasyMock.verify(metaData, connection, statement, resultSet);
			
			assert results.size() == 2 : results;
			
			Iterator<QualifiedName> iterator = results.iterator();
			QualifiedName sequence = iterator.next();
			String schema = sequence.getSchema();
			String name = sequence.getName();
			
			assert schema == null : schema;
			assert name.equals("sequence1") : name;

			sequence = iterator.next();
			schema = sequence.getSchema();
			name = sequence.getName();
			
			assert schema == null : schema;
			assert name.equals("sequence2") : name;
		}
		catch (SQLException e)
		{
			assert false : e;
		}
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
		
		String sql = this.getNextSequenceValueSQL(sequence);
		
		EasyMock.verify(sequence);
		
		assert sql.equals("SELECT GEN_ID(sequence, 1) FROM RDB$DATABASE") : sql;
	}

	/**
	 * @see net.sf.hajdbc.dialect.TestStandardDialect#testGetSimpleSQL()
	 */
	@Override
	public void testGetSimpleSQL()
	{
		String result = this.getSimpleSQL();
		
		assert result.equals("SELECT CURRENT_TIMESTAMP FROM RDB$DATABASE") : result;
	}

	@Override
	@DataProvider(name = "select-for-update-sql")
	Object[][] selectForUpdateProvider()
	{
		return new Object[][] {
			new Object[] { "SELECT * FROM success WITH LOCK" },
			new Object[] { "SELECT * FROM failure" },
		};
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
	 * @see net.sf.hajdbc.dialect.TestStandardDialect#testParseInsertTable(java.lang.String)
	 */
	@Override
	@Test(dataProvider = "insert-table-sql")
	public void testParseInsertTable(String sql)
	{
		String result = this.parseInsertTable(sql);
		
		assert result == null : result;
	}
}
