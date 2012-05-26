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

import net.sf.hajdbc.QualifiedName;
import net.sf.hajdbc.SequenceProperties;
import net.sf.hajdbc.SequenceSupport;

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
		super(DialectFactoryEnum.FIREBIRD);
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
		DatabaseMetaData metaData = mock(DatabaseMetaData.class);
		Connection connection = mock(Connection.class);
		Statement statement = mock(Statement.class);
		ResultSet resultSet = mock(ResultSet.class);
		
		when(metaData.getConnection()).thenReturn(connection);
		when(connection.createStatement()).thenReturn(statement);
		when(statement.executeQuery("SELECT RDB$GENERATOR_NAME FROM RDB$GENERATORS")).thenReturn(resultSet);
		when(resultSet.next()).thenReturn(true).thenReturn(true).thenReturn(false);
		when(resultSet.getString(1)).thenReturn("sequence1").thenReturn("sequence2");

		Map<QualifiedName, Integer> results = this.dialect.getSequenceSupport().getSequences(metaData);

		verify(statement).close();

		assertEquals(results.size(), 2);
		
		Iterator<Map.Entry<QualifiedName, Integer>> entries = results.entrySet().iterator();
		Map.Entry<QualifiedName, Integer> entry = entries.next();

		assertNull(entry.getKey().getSchema());
		assertEquals("sequence1", entry.getKey().getName());
		assertEquals(1, entry.getValue().intValue());
		
		entry = entries.next();
		
		assertNull(entry.getKey().getSchema());
		assertEquals("sequence2", entry.getKey().getName());
		assertEquals(1, entry.getValue().intValue());
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
