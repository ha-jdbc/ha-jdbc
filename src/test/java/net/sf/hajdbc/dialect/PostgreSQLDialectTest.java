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
import java.sql.Types;
import java.util.List;
import java.util.regex.Pattern;

import net.sf.hajdbc.ColumnProperties;
import net.sf.hajdbc.QualifiedName;
import net.sf.hajdbc.SequenceProperties;
import net.sf.hajdbc.SequenceSupport;
import net.sf.hajdbc.TableProperties;
import net.sf.hajdbc.dialect.postgresql.PostgreSQLDialectFactory;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 * @author Paul Ferraro
 *
 */
public class PostgreSQLDialectTest extends StandardDialectTest
{
	public PostgreSQLDialectTest()
	{
		super(new PostgreSQLDialectFactory());
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
	 * @see net.sf.hajdbc.dialect.StandardDialectTest#getIdentityColumnSupport()
	 */
	@Override
	public void getIdentityColumnSupport()
	{
		assertSame(this.dialect, this.dialect.getIdentityColumnSupport());
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.dialect.StandardDialectTest#getColumnType()
	 */
	@Override
	public void getColumnType() throws SQLException
	{
		ColumnProperties column = mock(ColumnProperties.class);
		
		when(column.getNativeType()).thenReturn("oid");
		
		int result = this.dialect.getColumnType(column);
		
		assertEquals(Types.BLOB, result);
		
		when(column.getNativeType()).thenReturn("int");		
		when(column.getType()).thenReturn(Types.INTEGER);
		
		result = this.dialect.getColumnType(column);
		
		assertEquals(Types.INTEGER, result);
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.dialect.StandardDialectTest#getTruncateTableSQL()
	 */
	@Override
	public void getTruncateTableSQL() throws SQLException
	{
		TableProperties table = mock(TableProperties.class);
		QualifiedName name = mock(QualifiedName.class);
		
		when(table.getName()).thenReturn(name);
		when(name.getDMLName()).thenReturn("table");
		
		String result = this.dialect.getTruncateTableSQL(table);
		
		assertEquals("TRUNCATE TABLE table", result);
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
		
		assertEquals("SELECT NEXTVAL('sequence')", result);
	}
	
	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.dialect.StandardDialectTest#parseSequence()
	 */
	@Override
	public void parseSequence() throws SQLException
	{
		SequenceSupport support = this.dialect.getSequenceSupport();
		
		assertEquals("sequence", support.parseSequence("SELECT CURRVAL('sequence')"));
		assertEquals("sequence", support.parseSequence("SELECT nextval('sequence'), * FROM table"));
		assertEquals("sequence", support.parseSequence("INSERT INTO table VALUES (NEXTVAL('sequence'), 0)"));
		assertEquals("sequence", support.parseSequence("UPDATE table SET id = NEXTVAL('sequence')"));
		assertNull(support.parseSequence("SELECT NEXT VALUE FOR sequence"));
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.dialect.StandardDialectTest#getDefaultSchemas()
	 */
	@Override
	public void getDefaultSchemas() throws SQLException
	{
		DatabaseMetaData metaData = mock(DatabaseMetaData.class);
		Connection connection = mock(Connection.class);
		Statement statement = mock(Statement.class);
		ResultSet resultSet = mock(ResultSet.class);
		
		when(metaData.getConnection()).thenReturn(connection);
		when(connection.createStatement()).thenReturn(statement);
		
		when(statement.executeQuery("SHOW search_path")).thenReturn(resultSet);
		when(resultSet.next()).thenReturn(false);
		when(resultSet.getString(1)).thenReturn("$user,public");

		resultSet.close();
		statement.close();
		
		when(metaData.getUserName()).thenReturn("user");
		
		List<String> result = this.dialect.getDefaultSchemas(metaData);

		assertEquals(2, result.size());
		assertEquals("user", result.get(0));
		assertEquals("public", result.get(1));
	}
	
	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.dialect.StandardDialectTest#getAlterIdentityColumnSQL()
	 */
	@Override
	public void getAlterIdentityColumnSQL() throws SQLException
	{
		TableProperties table = mock(TableProperties.class);
		ColumnProperties column = mock(ColumnProperties.class);
		QualifiedName name = mock(QualifiedName.class);
		
		when(table.getName()).thenReturn(name);
		when(name.getDDLName()).thenReturn("table");
		when(column.getName()).thenReturn("column");
		
		String result = this.dialect.getIdentityColumnSupport().getAlterIdentityColumnSQL(table, column, 1000L);

		assertEquals("ALTER SEQUENCE table_column_seq RESTART WITH 1000", result);
	}
	
	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.dialect.StandardDialectTest#getIdentifierPattern()
	 */
	@Override
	public void getIdentifierPattern() throws SQLException
	{
		DatabaseMetaData metaData = mock(DatabaseMetaData.class);
		
		when(metaData.getDriverMajorVersion()).thenReturn(8);
		when(metaData.getDriverMinorVersion()).thenReturn(0);
		
		when(metaData.getExtraNameCharacters()).thenReturn("$");
		
		String result = this.dialect.getIdentifierPattern(metaData).pattern();
		
		assert result.equals("[a-zA-Z][\\w\\Q$\\E]*") : result;
		
		when(metaData.getDriverMajorVersion()).thenReturn(8);
		when(metaData.getDriverMinorVersion()).thenReturn(1);
		
		result = this.dialect.getIdentifierPattern(metaData).pattern();
		
		assert result.equals("[A-Za-z\\0200-\\0377_][A-Za-z\\0200-\\0377_0-9\\$]*") : result;
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.dialect.StandardDialectTest#evaluateRand()
	 */
	@Override
	public void evaluateRand()
	{
		assertTrue(Pattern.matches("SELECT ((0\\.\\d+)|([1-9]\\.\\d+E\\-\\d+)) FROM test", this.dialect.evaluateRand("SELECT RANDOM() FROM test")));
		assertTrue(Pattern.matches("SELECT ((0\\.\\d+)|([1-9]\\.\\d+E\\-\\d+)) FROM test", this.dialect.evaluateRand("SELECT RANDOM ( ) FROM test")));
		assertEquals("SELECT RAND() FROM test", this.dialect.evaluateRand("SELECT RAND() FROM test"));
		assertEquals("SELECT OPERANDOM() FROM test", this.dialect.evaluateRand("SELECT OPERANDOM() FROM test"));
	}
}
