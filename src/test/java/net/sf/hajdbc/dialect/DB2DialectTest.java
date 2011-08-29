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
import java.util.Iterator;
import java.util.Map;

import net.sf.hajdbc.SequenceSupport;
import net.sf.hajdbc.cache.QualifiedName;
import net.sf.hajdbc.cache.SequenceProperties;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 * @author Paul Ferraro
 *
 */
public class DB2DialectTest extends StandardDialectTest
{
	public DB2DialectTest()
	{
		super(DialectFactoryEnum.DB2);
	}

	@Override
	public void getSequenceSupport()
	{
		assertSame(this.dialect, this.dialect.getSequenceSupport());
	}
	
	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.dialect.StandardDialectTest#getIdentityColumnSupport()
	 */
	@Override
	public void getIdentityColumnSupport()
	{
		assertSame(this.dialect, this.dialect.getIdentityColumnSupport());
	}

	@Override
	public void getSequences() throws SQLException
	{
		DatabaseMetaData metaData = mock(DatabaseMetaData.class);
		Connection connection = mock(Connection.class);
		Statement statement = mock(Statement.class);
		ResultSet resultSet = mock(ResultSet.class);
		
		when(metaData.getConnection()).thenReturn(connection);
		when(connection.createStatement()).thenReturn(statement);
		when(statement.executeQuery("SELECT SEQSCHEMA, SEQNAME, INCREMENT FROM SYSCAT.SEQUENCES")).thenReturn(resultSet);
		when(resultSet.next()).thenReturn(true).thenReturn(true).thenReturn(false);
		when(resultSet.getString(1)).thenReturn("schema1").thenReturn("schema2");
		when(resultSet.getString(2)).thenReturn("sequence1").thenReturn("sequence2");
		when(resultSet.getInt(3)).thenReturn(1).thenReturn(2);
		
		Map<QualifiedName, Integer> result = this.dialect.getSequenceSupport().getSequences(metaData);

		verify(statement).close();
		
		assertEquals(2, result.size());
		
		Iterator<Map.Entry<QualifiedName, Integer>> entries = result.entrySet().iterator();
		Map.Entry<QualifiedName, Integer> entry = entries.next();
		
		assertEquals("schema1", entry.getKey().getSchema());
		assertEquals("sequence1", entry.getKey().getName());
		assertEquals(1, entry.getValue().intValue());
		
		entry = entries.next();
		
		assertEquals("schema2", entry.getKey().getSchema());
		assertEquals("sequence2", entry.getKey().getName());
		assertEquals(2, entry.getValue().intValue());
	}

	@Override
	public void getNextSequenceValueSQL() throws SQLException
	{
		SequenceProperties sequence = mock(SequenceProperties.class);
		
		when(sequence.getName()).thenReturn("sequence");
		
		String result = this.dialect.getSequenceSupport().getNextSequenceValueSQL(sequence);
		
		assertEquals("VALUES NEXTVAL FOR sequence", result);
	}

	@Override
	public void getSimpleSQL() throws SQLException
	{
		assertEquals("VALUES CURRENT_TIMESTAMP", this.dialect.getSimpleSQL());
	}
	
	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.dialect.StandardDialectTest#parseSequence()
	 */
	@Override
	public void parseSequence() throws SQLException
	{
		SequenceSupport support = this.dialect.getSequenceSupport();
		assertEquals("test", support.parseSequence("VALUES NEXTVAL FOR test"));
		assertEquals("test", support.parseSequence("INSERT INTO table VALUES (NEXTVAL FOR test, 0)"));
		assertEquals("test", support.parseSequence("INSERT INTO table VALUES (PREVVAL FOR test, 0)"));
		assertEquals("test", support.parseSequence("UPDATE table SET id = NEXTVAL FOR test"));
		assertEquals("test", support.parseSequence("UPDATE table SET id = PREVVAL FOR test"));
		assertNull(support.parseSequence("SELECT * FROM test"));
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.dialect.StandardDialectTest#evaluateCurrentDate()
	 */
	@Override
	public void evaluateCurrentDate()
	{
		java.sql.Date date = new java.sql.Date(System.currentTimeMillis());
		
		assertEquals(String.format("SELECT '%s' FROM test", date.toString()), this.dialect.evaluateCurrentDate("SELECT CURRENT_DATE FROM test", date));
		assertEquals("SELECT CCURRENT_DATE FROM test", this.dialect.evaluateCurrentDate("SELECT CCURRENT_DATE FROM test", date));
		assertEquals("SELECT CURRENT_DATES FROM test", this.dialect.evaluateCurrentDate("SELECT CURRENT_DATES FROM test", date));
		assertEquals("SELECT CURRENT_TIME FROM test", this.dialect.evaluateCurrentDate("SELECT CURRENT_TIME FROM test", date));
		assertEquals("SELECT CURRENT_TIMESTAMP FROM test", this.dialect.evaluateCurrentDate("SELECT CURRENT_TIMESTAMP FROM test", date));
		assertEquals("SELECT 1 FROM test", this.dialect.evaluateCurrentDate("SELECT 1 FROM test", date));
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.dialect.StandardDialectTest#evaluateCurrentTime()
	 */
	@Override
	public void evaluateCurrentTime()
	{
		java.sql.Time time = new java.sql.Time(System.currentTimeMillis());
		
		assertEquals(String.format("SELECT '%s' FROM test", time.toString()), this.dialect.evaluateCurrentTime("SELECT CURRENT_TIME FROM test", time));
		assertEquals(String.format("SELECT '%s' FROM test", time.toString()), this.dialect.evaluateCurrentTime("SELECT CURRENT_TIME(2) FROM test", time));
		assertEquals(String.format("SELECT '%s' FROM test", time.toString()), this.dialect.evaluateCurrentTime("SELECT CURRENT_TIME ( 2 ) FROM test", time));
		assertEquals(String.format("SELECT '%s' FROM test", time.toString()), this.dialect.evaluateCurrentTime("SELECT LOCALTIME FROM test", time));
		assertEquals(String.format("SELECT '%s' FROM test", time.toString()), this.dialect.evaluateCurrentTime("SELECT LOCALTIME(2) FROM test", time));
		assertEquals(String.format("SELECT '%s' FROM test", time.toString()), this.dialect.evaluateCurrentTime("SELECT LOCALTIME ( 2 ) FROM test", time));
		assertEquals("SELECT CCURRENT_TIME FROM test", this.dialect.evaluateCurrentTime("SELECT CCURRENT_TIME FROM test", time));
		assertEquals("SELECT LLOCALTIME FROM test", this.dialect.evaluateCurrentTime("SELECT LLOCALTIME FROM test", time));
		assertEquals("SELECT CURRENT_DATE FROM test", this.dialect.evaluateCurrentTime("SELECT CURRENT_DATE FROM test", time));
		assertEquals("SELECT CURRENT_TIMESTAMP FROM test", this.dialect.evaluateCurrentTime("SELECT CURRENT_TIMESTAMP FROM test", time));
		assertEquals("SELECT LOCALTIMESTAMP FROM test", this.dialect.evaluateCurrentTime("SELECT LOCALTIMESTAMP FROM test", time));
		assertEquals("SELECT 1 FROM test", this.dialect.evaluateCurrentTime("SELECT 1 FROM test", time));
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.dialect.StandardDialectTest#evaluateCurrentTimestamp()
	 */
	@Override
	public void evaluateCurrentTimestamp()
	{
		java.sql.Timestamp timestamp = new java.sql.Timestamp(System.currentTimeMillis());
		
		assertEquals(String.format("SELECT '%s' FROM test", timestamp.toString()), this.dialect.evaluateCurrentTimestamp("SELECT CURRENT_TIMESTAMP FROM test", timestamp));
		assertEquals(String.format("SELECT '%s' FROM test", timestamp.toString()), this.dialect.evaluateCurrentTimestamp("SELECT CURRENT_TIMESTAMP(2) FROM test", timestamp));
		assertEquals(String.format("SELECT '%s' FROM test", timestamp.toString()), this.dialect.evaluateCurrentTimestamp("SELECT CURRENT_TIMESTAMP ( 2 ) FROM test", timestamp));
		assertEquals(String.format("SELECT '%s' FROM test", timestamp.toString()), this.dialect.evaluateCurrentTimestamp("SELECT LOCALTIMESTAMP FROM test", timestamp));
		assertEquals(String.format("SELECT '%s' FROM test", timestamp.toString()), this.dialect.evaluateCurrentTimestamp("SELECT LOCALTIMESTAMP(2) FROM test", timestamp));
		assertEquals(String.format("SELECT '%s' FROM test", timestamp.toString()), this.dialect.evaluateCurrentTimestamp("SELECT LOCALTIMESTAMP ( 2 ) FROM test", timestamp));
		assertEquals("SELECT CCURRENT_TIMESTAMP FROM test", this.dialect.evaluateCurrentTimestamp("SELECT CCURRENT_TIMESTAMP FROM test", timestamp));
		assertEquals("SELECT LLOCALTIMESTAMP FROM test", this.dialect.evaluateCurrentTimestamp("SELECT LLOCALTIMESTAMP FROM test", timestamp));
		assertEquals("SELECT CURRENT_DATE FROM test", this.dialect.evaluateCurrentTimestamp("SELECT CURRENT_DATE FROM test", timestamp));
		assertEquals("SELECT CURRENT_TIME FROM test", this.dialect.evaluateCurrentTimestamp("SELECT CURRENT_TIME FROM test", timestamp));
		assertEquals("SELECT LOCALTIME FROM test", this.dialect.evaluateCurrentTimestamp("SELECT LOCALTIME FROM test", timestamp));
		assertEquals("SELECT 1 FROM test", this.dialect.evaluateCurrentTimestamp("SELECT 1 FROM test", timestamp));
	}
}
