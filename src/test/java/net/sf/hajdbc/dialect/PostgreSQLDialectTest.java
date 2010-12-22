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

import net.sf.hajdbc.SequenceSupport;
import net.sf.hajdbc.cache.ColumnProperties;
import net.sf.hajdbc.cache.SequenceProperties;
import net.sf.hajdbc.cache.TableProperties;

import org.easymock.EasyMock;
import org.easymock.IMocksControl;
import org.junit.Assert;

/**
 * @author Paul Ferraro
 *
 */
public class PostgreSQLDialectTest extends StandardDialectTest
{
	public PostgreSQLDialectTest()
	{
		super(DialectFactoryEnum.POSTGRESQL);
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.dialect.StandardDialectTest#getSequenceSupport()
	 */
	@Override
	public void getSequenceSupport()
	{
		Assert.assertSame(this.dialect, this.dialect.getSequenceSupport());
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.dialect.StandardDialectTest#getIdentityColumnSupport()
	 */
	@Override
	public void getIdentityColumnSupport()
	{
		Assert.assertSame(this.dialect, this.dialect.getIdentityColumnSupport());
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.dialect.StandardDialectTest#getColumnType()
	 */
	@Override
	public void getColumnType() throws SQLException
	{
		ColumnProperties column = EasyMock.createStrictMock(ColumnProperties.class);
		
		EasyMock.expect(column.getNativeType()).andReturn("oid");
		
		EasyMock.replay(column);
		
		int result = this.dialect.getColumnType(column);
		
		EasyMock.verify(column);
		
		Assert.assertEquals(Types.BLOB, result);
		
		EasyMock.reset(column);
		
		EasyMock.expect(column.getNativeType()).andReturn("int");		
		EasyMock.expect(column.getType()).andReturn(Types.INTEGER);
		
		EasyMock.replay(column);
		
		result = this.dialect.getColumnType(column);
		
		EasyMock.verify(column);
		
		Assert.assertEquals(Types.INTEGER, result);
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.dialect.StandardDialectTest#getTruncateTableSQL()
	 */
	@Override
	public void getTruncateTableSQL() throws SQLException
	{
		TableProperties table = EasyMock.createStrictMock(TableProperties.class);
		
		EasyMock.expect(table.getName()).andReturn("table");
		
		EasyMock.replay(table);
		
		String result = this.dialect.getTruncateTableSQL(table);
		
		EasyMock.verify(table);
		
		Assert.assertEquals("TRUNCATE TABLE table", result);
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.dialect.StandardDialectTest#getNextSequenceValueSQL()
	 */
	@Override
	public void getNextSequenceValueSQL() throws SQLException
	{
		SequenceProperties sequence = EasyMock.createStrictMock(SequenceProperties.class);
		
		EasyMock.expect(sequence.getName()).andReturn("sequence");
		
		EasyMock.replay(sequence);
		
		String result = this.dialect.getSequenceSupport().getNextSequenceValueSQL(sequence);
		
		EasyMock.verify(sequence);
		
		Assert.assertEquals("SELECT NEXTVAL('sequence')", result);
	}
	
	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.dialect.StandardDialectTest#parseSequence()
	 */
	@Override
	public void parseSequence() throws SQLException
	{
		SequenceSupport support = this.dialect.getSequenceSupport();
		
		Assert.assertEquals("sequence", support.parseSequence("SELECT CURRVAL('sequence')"));
		Assert.assertEquals("sequence", support.parseSequence("SELECT nextval('sequence'), * FROM table"));
		Assert.assertEquals("sequence", support.parseSequence("INSERT INTO table VALUES (NEXTVAL('sequence'), 0)"));
		Assert.assertEquals("sequence", support.parseSequence("UPDATE table SET id = NEXTVAL('sequence')"));
		Assert.assertNull(support.parseSequence("SELECT NEXT VALUE FOR sequence"));
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.dialect.StandardDialectTest#getDefaultSchemas()
	 */
	@Override
	public void getDefaultSchemas() throws SQLException
	{
		IMocksControl control = EasyMock.createStrictControl();
		DatabaseMetaData metaData = control.createMock(DatabaseMetaData.class);
		Connection connection = control.createMock(Connection.class);
		Statement statement = control.createMock(Statement.class);
		ResultSet resultSet = control.createMock(ResultSet.class);
		
		EasyMock.expect(metaData.getConnection()).andReturn(connection);
		EasyMock.expect(connection.createStatement()).andReturn(statement);
		
		EasyMock.expect(statement.executeQuery("SHOW search_path")).andReturn(resultSet);
		EasyMock.expect(resultSet.next()).andReturn(false);
		EasyMock.expect(resultSet.getString(1)).andReturn("$user,public");

		resultSet.close();
		statement.close();
		
		EasyMock.expect(metaData.getUserName()).andReturn("user");
		
		control.replay();
		
		List<String> result = this.dialect.getDefaultSchemas(metaData);

		control.verify();

		Assert.assertEquals(2, result.size());
		Assert.assertEquals("user", result.get(0));
		Assert.assertEquals("public", result.get(1));
	}
	
	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.dialect.StandardDialectTest#getAlterIdentityColumnSQL()
	 */
	@Override
	public void getAlterIdentityColumnSQL() throws SQLException
	{
		IMocksControl control = EasyMock.createStrictControl();
		TableProperties table = control.createMock(TableProperties.class);
		ColumnProperties column = control.createMock(ColumnProperties.class);
		
		EasyMock.expect(table.getName()).andReturn("table");
		EasyMock.expect(column.getName()).andReturn("column");
		
		control.replay();
		
		String result = this.dialect.getIdentityColumnSupport().getAlterIdentityColumnSQL(table, column, 1000L);

		control.verify();

		Assert.assertEquals("ALTER SEQUENCE table_column_seq RESTART WITH 1000", result);
	}
	
	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.dialect.StandardDialectTest#getIdentifierPattern()
	 */
	@Override
	public void getIdentifierPattern() throws SQLException
	{
		DatabaseMetaData metaData = EasyMock.createStrictMock(DatabaseMetaData.class);
		
		EasyMock.expect(metaData.getDriverMajorVersion()).andReturn(8);
		EasyMock.expect(metaData.getDriverMinorVersion()).andReturn(0);
		
		EasyMock.expect(metaData.getExtraNameCharacters()).andReturn("$");
		
		EasyMock.replay(metaData);
		
		String result = this.dialect.getIdentifierPattern(metaData).pattern();
		
		EasyMock.verify(metaData);
		
		assert result.equals("[a-zA-Z][\\w\\Q$\\E]*") : result;
		
		EasyMock.reset(metaData);
		
		EasyMock.expect(metaData.getDriverMajorVersion()).andReturn(8);
		EasyMock.expect(metaData.getDriverMinorVersion()).andReturn(1);
		
		EasyMock.replay(metaData);
		
		result = this.dialect.getIdentifierPattern(metaData).pattern();
		
		EasyMock.verify(metaData);
		
		assert result.equals("[A-Za-z\\0200-\\0377_][A-Za-z\\0200-\\0377_0-9\\$]*") : result;
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.dialect.StandardDialectTest#evaluateRand()
	 */
	@Override
	public void evaluateRand()
	{
		Assert.assertTrue(Pattern.matches("SELECT ((0\\.\\d+)|([1-9]\\.\\d+E\\-\\d+)) FROM test", this.dialect.evaluateRand("SELECT RANDOM() FROM test")));
		Assert.assertTrue(Pattern.matches("SELECT ((0\\.\\d+)|([1-9]\\.\\d+E\\-\\d+)) FROM test", this.dialect.evaluateRand("SELECT RANDOM ( ) FROM test")));
		Assert.assertEquals("SELECT RAND() FROM test", this.dialect.evaluateRand("SELECT RAND() FROM test"));
		Assert.assertEquals("SELECT OPERANDOM() FROM test", this.dialect.evaluateRand("SELECT OPERANDOM() FROM test"));
	}
}
