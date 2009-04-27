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

import net.sf.hajdbc.ColumnProperties;
import net.sf.hajdbc.SequenceProperties;
import net.sf.hajdbc.TableProperties;

import org.easymock.EasyMock;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * @author Paul Ferraro
 *
 */
@SuppressWarnings("nls")
@Test
public class TestPostgreSQLDialect extends TestStandardDialect
{
	public TestPostgreSQLDialect()
	{
		super(new PostgreSQLDialect());
	}

	/**
	 * @see net.sf.hajdbc.dialect.TestStandardDialect#testGetColumnType()
	 */
	@Override
	public void testGetColumnType() throws SQLException
	{
		ColumnProperties column = EasyMock.createStrictMock(ColumnProperties.class);
		
		EasyMock.expect(column.getNativeType()).andReturn("oid");
		
		EasyMock.replay(column);
		
		int result = this.getColumnType(column);
		
		EasyMock.verify(column);
		
		assert result == Types.BLOB : result;
		
		EasyMock.reset(column);
		
		EasyMock.expect(column.getNativeType()).andReturn("int");		
		EasyMock.expect(column.getType()).andReturn(Types.INTEGER);
		
		EasyMock.replay(column);
		
		result = this.getColumnType(column);
		
		EasyMock.verify(column);
		
		assert result == Types.INTEGER : result;
	}

	/**
	 * @see net.sf.hajdbc.dialect.TestStandardDialect#testGetTruncateTableSQL()
	 */
	@Override
	public void testGetTruncateTableSQL() throws SQLException
	{
		TableProperties table = EasyMock.createStrictMock(TableProperties.class);
		
		EasyMock.expect(table.getName()).andReturn("table");
		
		EasyMock.replay(table);
		
		String result = this.getTruncateTableSQL(table);
		
		EasyMock.verify(table);
		
		assert result.equals("TRUNCATE TABLE table") : result;
	}

	/**
	 * @see net.sf.hajdbc.dialect.TestStandardDialect#testGetNextSequenceValueSQL()
	 */
	@Override
	public void testGetNextSequenceValueSQL() throws SQLException
	{
		SequenceProperties sequence = EasyMock.createStrictMock(SequenceProperties.class);
		
		EasyMock.expect(sequence.getName()).andReturn("sequence");
		
		EasyMock.replay(sequence);
		
		String result = this.getNextSequenceValueSQL(sequence);
		
		EasyMock.verify(sequence);
		
		assert result.equals("SELECT NEXTVAL('sequence')") : result;
	}
	
	@Override
	@DataProvider(name = "sequence-sql")
	Object[][] sequenceSQLProvider()
	{
		return new Object[][] {
			new Object[] { "SELECT CURRVAL('success')" },
			new Object[] { "SELECT nextval('success'), * FROM table" },
			new Object[] { "INSERT INTO table VALUES (NEXTVAL('success'), 0)" },
			new Object[] { "UPDATE table SET id = NEXTVAL('success')" },
			new Object[] { "SELECT * FROM table" },
		};
	}

	/**
	 * @see net.sf.hajdbc.dialect.TestStandardDialect#testGetDefaultSchemas()
	 */
	@Override
	public void testGetDefaultSchemas() throws SQLException
	{
		DatabaseMetaData metaData = EasyMock.createStrictMock(DatabaseMetaData.class);
		Connection connection = EasyMock.createStrictMock(Connection.class);
		Statement statement = EasyMock.createStrictMock(Statement.class);
		ResultSet resultSet = EasyMock.createStrictMock(ResultSet.class);
		
		EasyMock.expect(metaData.getConnection()).andReturn(connection);
		EasyMock.expect(connection.createStatement()).andReturn(statement);
		
		EasyMock.expect(statement.executeQuery("SHOW search_path")).andReturn(resultSet);
		EasyMock.expect(resultSet.next()).andReturn(false);
		EasyMock.expect(resultSet.getString(1)).andReturn("$user,public");

		resultSet.close();
		statement.close();
		
		EasyMock.expect(metaData.getUserName()).andReturn("user");
		
		EasyMock.replay(metaData, connection, statement, resultSet);
		
		List<String> result = this.getDefaultSchemas(metaData);
		
		EasyMock.verify(metaData, connection, statement, resultSet);
		
		assert result.size() == 2 : result.size();
		
		assert result.get(0).equals("user") : result.get(0);
		assert result.get(1).equals("public") : result.get(1);
	}
	
	/**
	 * @see net.sf.hajdbc.dialect.TestStandardDialect#testGetAlterIdentityColumnSQL()
	 */
	@Override
	public void testGetAlterIdentityColumnSQL() throws SQLException
	{
		TableProperties table = EasyMock.createStrictMock(TableProperties.class);
		ColumnProperties column = EasyMock.createStrictMock(ColumnProperties.class);
		
		EasyMock.expect(table.getName()).andReturn("table");
		EasyMock.expect(column.getName()).andReturn("column");
		
		EasyMock.replay(table, column);
		
		String result = this.getAlterIdentityColumnSQL(table, column, 1000L);
		
		EasyMock.verify(table, column);
		
		assert result.equals("ALTER SEQUENCE table_column_seq RESTART WITH 1000") : result;
	}
	
	/**
	 * @see net.sf.hajdbc.dialect.TestStandardDialect#testGetIdentifierPattern()
	 */
	@Override
	public void testGetIdentifierPattern() throws SQLException
	{
		DatabaseMetaData metaData = EasyMock.createStrictMock(DatabaseMetaData.class);
		
		EasyMock.expect(metaData.getDriverMajorVersion()).andReturn(8);
		EasyMock.expect(metaData.getDriverMinorVersion()).andReturn(0);
		
		EasyMock.expect(metaData.getExtraNameCharacters()).andReturn("");
		
		EasyMock.replay(metaData);
		
		String result = this.getIdentifierPattern(metaData).pattern();
		
		EasyMock.verify(metaData);
		
		assert result.equals("[\\w\\Q\\E]+") : result;
		
		EasyMock.reset(metaData);
		
		EasyMock.expect(metaData.getDriverMajorVersion()).andReturn(8);
		EasyMock.expect(metaData.getDriverMinorVersion()).andReturn(1);
		
		EasyMock.replay(metaData);
		
		result = this.getIdentifierPattern(metaData).pattern();
		
		EasyMock.verify(metaData);
		
		assert result.equals("[A-Za-z\\0200-\\0377_][A-Za-z\\0200-\\0377_0-9\\$]*") : result;
	}

	@Override
	@DataProvider(name = "random")
	Object[][] randomProvider()
	{
		return new Object[][] {
			new Object[] { "SELECT RANDOM() FROM success" },
			new Object[] { "SELECT RANDOM ( ) FROM success" },
			new Object[] { "SELECT OPERANDOM() FROM failure" },
			new Object[] { "SELECT 1 FROM failure" },
		};
	}
}
