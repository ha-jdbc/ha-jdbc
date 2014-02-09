/*
 * HA-JDBC: High-Availability JDBC
 * Copyright (C) 2012  Paul Ferraro
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package net.sf.hajdbc.dialect;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;
import java.util.Iterator;
import java.util.regex.Pattern;

import net.sf.hajdbc.SequenceProperties;
import net.sf.hajdbc.SequencePropertiesFactory;
import net.sf.hajdbc.SequenceSupport;
import net.sf.hajdbc.dialect.ingres.IngresDialectFactory;

/**
 * @author Paul Ferraro
 */
@SuppressWarnings("nls")
public class IngresDialectTest extends StandardDialectTest
{
	public IngresDialectTest()
	{
		super(new IngresDialectFactory());
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.dialect.StandardDialectTest#getSequenceSupport()
	 */
	@Override
	public void getSequenceSupport()
	{
		assertSame(this.dialect, this.dialect.getSequenceSupport());
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.dialect.StandardDialectTest#getSequences()
	 */
	@Override
	public void getSequences() throws SQLException
	{
		SequencePropertiesFactory factory = mock(SequencePropertiesFactory.class);
		SequenceProperties sequence1 = mock(SequenceProperties.class);
		SequenceProperties sequence2 = mock(SequenceProperties.class);
		DatabaseMetaData metaData = mock(DatabaseMetaData.class);
		Connection connection = mock(Connection.class);
		Statement statement = mock(Statement.class);
		ResultSet resultSet = mock(ResultSet.class);
		
		when(metaData.getConnection()).thenReturn(connection);
		when(connection.createStatement()).thenReturn(statement);
		when(statement.executeQuery("SELECT seq_name FROM iisequence")).thenReturn(resultSet);
		when(resultSet.next()).thenReturn(true).thenReturn(true).thenReturn(false);
		when(resultSet.getString(1)).thenReturn("sequence1").thenReturn("sequence2");
		when(factory.createSequenceProperties(null, "sequence1", 1)).thenReturn(sequence1);
		when(factory.createSequenceProperties(null, "sequence2", 1)).thenReturn(sequence2);

		Collection<SequenceProperties> results = this.dialect.getSequenceSupport().getSequences(metaData, factory);
		
		verify(statement).close();
		
		assertEquals(2, results.size());
		
		Iterator<SequenceProperties> sequences = results.iterator();

		assertSame(sequence1, sequences.next());
		assertSame(sequence2, sequences.next());
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.dialect.StandardDialectTest#parseSequence()
	 */
	@Override
	public void parseSequence() throws SQLException
	{
		SequenceSupport support = this.dialect.getSequenceSupport();
		assertEquals("sequence", support.parseSequence("SELECT NEXT VALUE FOR sequence"));
		assertEquals("sequence", support.parseSequence("SELECT CURRENT VALUE FOR sequence"));
		assertEquals("sequence", support.parseSequence("SELECT NEXT VALUE FOR sequence, * FROM table"));
		assertEquals("sequence", support.parseSequence("SELECT CURRENT VALUE FOR sequence, * FROM table"));
		assertEquals("sequence", support.parseSequence("INSERT INTO table VALUES (NEXT VALUE FOR sequence, 0)"));
		assertEquals("sequence", support.parseSequence("INSERT INTO table VALUES (CURRENT VALUE FOR sequence, 0)"));
		assertEquals("sequence", support.parseSequence("UPDATE table SET id = NEXT VALUE FOR sequence"));
		assertEquals("sequence", support.parseSequence("UPDATE table SET id = CURRENT VALUE FOR sequence"));
		assertEquals("sequence", support.parseSequence("SELECT sequence.nextval"));
		assertEquals("sequence", support.parseSequence("SELECT sequence.currval"));
		assertEquals("sequence", support.parseSequence("SELECT sequence.nextval, * FROM table"));
		assertEquals("sequence", support.parseSequence("SELECT sequence.currval, * FROM table"));
		assertEquals("sequence", support.parseSequence("INSERT INTO table VALUES (sequence.nextval, 0)"));
		assertEquals("sequence", support.parseSequence("INSERT INTO table VALUES (sequence.currval, 0)"));
		assertEquals("sequence", support.parseSequence("UPDATE table SET id = sequence.nextval"));
		assertEquals("sequence", support.parseSequence("UPDATE table SET id = sequence.currval"));
		assertNull(support.parseSequence("SELECT * FROM table"));
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.dialect.StandardDialectTest#evaluateCurrentDate()
	 */
	@Override
	public void evaluateCurrentDate()
	{
		java.sql.Date date = new java.sql.Date(System.currentTimeMillis());
		
		assertEquals(String.format("SELECT DATE '%s' FROM test", date.toString()), this.dialect.evaluateCurrentDate("SELECT CURRENT_DATE FROM test", date));
		assertEquals(String.format("SELECT DATE '%s' FROM test", date.toString()), this.dialect.evaluateCurrentDate("SELECT DATE('TODAY') FROM test", date));
		assertEquals(String.format("SELECT DATE '%s' FROM test", date.toString()), this.dialect.evaluateCurrentDate("SELECT DATE ( 'TODAY' ) FROM test", date));
		assertEquals("SELECT CURRENT_DATES FROM test", this.dialect.evaluateCurrentDate("SELECT CURRENT_DATES FROM test", date));
		assertEquals("SELECT CCURRENT_DATE FROM test", this.dialect.evaluateCurrentDate("SELECT CCURRENT_DATE FROM test", date));
		assertEquals("SELECT CURRENT_TIME FROM test", this.dialect.evaluateCurrentDate("SELECT CURRENT_TIME FROM test", date));
		assertEquals("SELECT CURRENT_TIMESTAMP FROM test", this.dialect.evaluateCurrentDate("SELECT CURRENT_TIMESTAMP FROM test", date));
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.dialect.StandardDialectTest#evaluateCurrentTime()
	 */
	@Override
	public void evaluateCurrentTime()
	{
		java.sql.Time time = new java.sql.Time(System.currentTimeMillis());
		
		assertEquals(String.format("SELECT TIME '%s' FROM test", time.toString()), this.dialect.evaluateCurrentTime("SELECT CURRENT_TIME FROM test", time));
		assertEquals(String.format("SELECT TIME '%s' FROM test", time.toString()), this.dialect.evaluateCurrentTime("SELECT LOCAL_TIME FROM test", time));
		assertEquals("SELECT CURRENT_TIMES FROM test", this.dialect.evaluateCurrentTime("SELECT CURRENT_TIMES FROM test", time));
		assertEquals("SELECT CCURRENT_TIME FROM test", this.dialect.evaluateCurrentTime("SELECT CCURRENT_TIME FROM test", time));
		assertEquals("SELECT CURRENT_DATE FROM test", this.dialect.evaluateCurrentTime("SELECT CURRENT_DATE FROM test", time));
		assertEquals("SELECT CURRENT_TIMESTAMP FROM test", this.dialect.evaluateCurrentTime("SELECT CURRENT_TIMESTAMP FROM test", time));
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.dialect.StandardDialectTest#evaluateCurrentTimestamp()
	 */
	@Override
	public void evaluateCurrentTimestamp()
	{
		java.sql.Timestamp timestamp = new java.sql.Timestamp(System.currentTimeMillis());
		
		assertEquals(String.format("SELECT TIMESTAMP '%s' FROM test", timestamp.toString()), this.dialect.evaluateCurrentTimestamp("SELECT CURRENT_TIMESTAMP FROM test", timestamp));
		assertEquals(String.format("SELECT TIMESTAMP '%s' FROM test", timestamp.toString()), this.dialect.evaluateCurrentTimestamp("SELECT LOCAL_TIMESTAMP FROM test", timestamp));
		assertEquals(String.format("SELECT TIMESTAMP '%s' FROM test", timestamp.toString()), this.dialect.evaluateCurrentTimestamp("SELECT DATE('NOW') FROM test", timestamp));
		assertEquals(String.format("SELECT TIMESTAMP '%s' FROM test", timestamp.toString()), this.dialect.evaluateCurrentTimestamp("SELECT DATE ( 'NOW' ) FROM test", timestamp));
		assertEquals("SELECT CURRENT_TIMESTAMPS FROM test", this.dialect.evaluateCurrentTimestamp("SELECT CURRENT_TIMESTAMPS FROM test", timestamp));
		assertEquals("SELECT CCURRENT_TIMESTAMP FROM test", this.dialect.evaluateCurrentTimestamp("SELECT CCURRENT_TIMESTAMP FROM test", timestamp));
		assertEquals("SELECT CURRENT_DATE FROM test", this.dialect.evaluateCurrentTimestamp("SELECT CURRENT_DATE FROM test", timestamp));
		assertEquals("SELECT CURRENT_TIME FROM test", this.dialect.evaluateCurrentTimestamp("SELECT CURRENT_TIME FROM test", timestamp));
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.dialect.StandardDialectTest#evaluateRand()
	 */
	@Override
	public void evaluateRand()
	{
		assertTrue(Pattern.matches("SELECT ((0\\.\\d+)|([1-9]\\.\\d+E\\-\\d+)) FROM test", this.dialect.evaluateRand("SELECT RANDOMF() FROM test")));
		assertTrue(Pattern.matches("SELECT ((0\\.\\d+)|([1-9]\\.\\d+E\\-\\d+)) FROM test", this.dialect.evaluateRand("SELECT RANDOMF ( ) FROM test")));
		assertEquals("SELECT RANDOMF FROM test", this.dialect.evaluateRand("SELECT RANDOMF FROM test"));
		assertEquals("SELECT OPERANDOMF() FROM test", this.dialect.evaluateRand("SELECT OPERANDOMF() FROM test"));
		assertEquals("SELECT RAND() FROM test", this.dialect.evaluateRand("SELECT RAND() FROM test"));
	}
}
