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
public class TestDB2Dialect extends TestStandardDialect
{
	public TestDB2Dialect()
	{
		super(new DB2Dialect());
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
			EasyMock.expect(statement.executeQuery("SELECT SEQSCHEMA, SEQNAME FROM SYSCAT.SEQUENCES")).andReturn(resultSet);
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
			
			assert result.size() == 2 : result.size();
			
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
		
		String result = this.getNextSequenceValueSQL(sequence);
		
		EasyMock.verify(sequence);
		
		assert result.equals("VALUES NEXTVAL FOR sequence") : result;
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

	@Override
	@DataProvider(name = "sequence-sql")
	Object[][] sequenceSQLProvider()
	{
		return new Object[][] {
			new Object[] { "VALUES NEXTVAL FOR success" },
			new Object[] { "VALUES PREVVAL FOR success" },
			new Object[] { "INSERT INTO table VALUES (NEXTVAL FOR success, 0)" },
			new Object[] { "INSERT INTO table VALUES (PREVVAL FOR success, 0)" },
			new Object[] { "UPDATE table SET id = NEXTVAL FOR success" },
			new Object[] { "UPDATE table SET id = PREVVAL FOR success" },
			new Object[] { "SELECT * FROM table" },
		};
	}
	
	@Override
	@Test(dataProvider = "current-date")
	public void testEvaluateCurrentDate(String sql, java.sql.Date date)
	{
		String expected = sql.contains("success") ? String.format("SELECT '%s' FROM success", date.toString()) : sql;
		
		String evaluated = this.evaluateCurrentDate(sql, date);

		assert evaluated.equals(expected) : evaluated;
	}
	
	@Override
	@Test(dataProvider = "current-time")
	public void testEvaluateCurrentTime(String sql, java.sql.Time date)
	{
		String expected = sql.contains("success") ? String.format("SELECT '%s' FROM success", date.toString()) : sql;
		
		String evaluated = this.evaluateCurrentTime(sql, date);

		assert evaluated.equals(expected) : evaluated;
	}
	
	@Override
	@Test(dataProvider = "current-timestamp")
	public void testEvaluateCurrentTimestamp(String sql, java.sql.Timestamp date)
	{
		String expected = sql.contains("success") ? String.format("SELECT '%s' FROM success", date.toString()) : sql;
		
		String evaluated = this.evaluateCurrentTimestamp(sql, date);
	
		assert evaluated.equals(expected) : evaluated;
	}
}
