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

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;
import java.util.Iterator;

import net.sf.hajdbc.QualifiedName;
import net.sf.hajdbc.SequenceProperties;
import net.sf.hajdbc.SequencePropertiesFactory;
import net.sf.hajdbc.SequenceSupport;
import net.sf.hajdbc.dialect.firebird.FirebirdDialectFactory;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 * @author Paul Ferraro
 *
 */
@SuppressWarnings("nls")
public class FirebirdDialectTest extends StandardDialectTest
{
	public FirebirdDialectTest()
	{
		super(new FirebirdDialectFactory());
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
	 * @see net.sf.hajdbc.dialect.StandardDialectTest#getAlterSequenceSQL()
	 */
	@Override
	public void getAlterSequenceSQL() throws SQLException
	{
		SequenceProperties sequence = mock(SequenceProperties.class);
		QualifiedName name = mock(QualifiedName.class);
		
		when(sequence.getName()).thenReturn(name);
		when(name.getDDLName()).thenReturn("sequence");
		when(sequence.getIncrement()).thenReturn(1);
		
		String result = this.dialect.getSequenceSupport().getAlterSequenceSQL(sequence, 1000L);

		assertEquals("SET GENERATOR sequence TO 1000", result);
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
		when(statement.executeQuery("SELECT RDB$GENERATOR_NAME FROM RDB$GENERATORS")).thenReturn(resultSet);
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
	 * @see net.sf.hajdbc.dialect.StandardDialectTest#getNextSequenceValueSQL()
	 */
	@Override
	public void getNextSequenceValueSQL() throws SQLException
	{
		SequenceProperties sequence = mock(SequenceProperties.class);
		QualifiedName name = mock(QualifiedName.class);
		
		when(sequence.getName()).thenReturn(name);
		when(name.getDMLName()).thenReturn("sequence");
		
		String result = this.dialect.getSequenceSupport().getNextSequenceValueSQL(sequence);
		
		assertEquals("SELECT GEN_ID(sequence, 1) FROM RDB$DATABASE", result);
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.dialect.StandardDialectTest#getSimpleSQL()
	 */
	@Override
	public void getSimpleSQL() throws SQLException
	{
		assertEquals("SELECT CURRENT_TIMESTAMP FROM RDB$DATABASE", this.dialect.getSimpleSQL());
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.dialect.StandardDialectTest#isSelectForUpdate()
	 */
	@Override
	public void isSelectForUpdate() throws SQLException
	{
		assertTrue(this.dialect.isSelectForUpdate("SELECT * FROM test WITH LOCK"));
		assertFalse(this.dialect.isSelectForUpdate("SELECT * FROM test"));
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.dialect.StandardDialectTest#parseSequence()
	 */
	@Override
	public void parseSequence() throws SQLException
	{
		SequenceSupport support = this.dialect.getSequenceSupport();
		assertEquals("sequence", support.parseSequence("SELECT GEN_ID(sequence, 1) FROM RDB$DATABASE"));
		assertEquals("sequence", support.parseSequence("SELECT GEN_ID(sequence, 1), * FROM table"));
		assertEquals("sequence", support.parseSequence("INSERT INTO table VALUES (GEN_ID(sequence, 1), 0)"));
		assertEquals("sequence", support.parseSequence("UPDATE table SET id = GEN_ID(sequence, 1)"));
		assertNull(support.parseSequence("SELECT NEXT VALUE FOR test"));
		assertNull(support.parseSequence("SELECT NEXT VALUE FOR test, * FROM table"));
		assertNull(support.parseSequence("INSERT INTO table VALUES (NEXT VALUE FOR test)"));
		assertNull(support.parseSequence("UPDATE table SET id = NEXT VALUE FOR test"));
		assertNull(support.parseSequence("SELECT * FROM table"));
	}
}
