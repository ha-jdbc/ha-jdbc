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

import net.sf.hajdbc.ForeignKeyConstraint;
import net.sf.hajdbc.QualifiedName;
import net.sf.hajdbc.SequenceProperties;
import net.sf.hajdbc.TableProperties;
import net.sf.hajdbc.cache.ForeignKeyConstraintImpl;

import org.easymock.EasyMock;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * @author Paul Ferraro
 *
 */
@SuppressWarnings("nls")
@Test
public class TestMaxDBDialect extends TestStandardDialect
{
	public TestMaxDBDialect()
	{
		super(new MaxDBDialect());
	}

	/**
	 * @see net.sf.hajdbc.dialect.TestStandardDialect#testGetCreateForeignKeyConstraintSQL()
	 */
	@Override
	public void testGetCreateForeignKeyConstraintSQL()
	{
		ForeignKeyConstraint key = new ForeignKeyConstraintImpl("name", "table");
		key.getColumnList().add("column1");
		key.getColumnList().add("column2");
		key.setForeignTable("foreign_table");
		key.getForeignColumnList().add("foreign_column1");
		key.getForeignColumnList().add("foreign_column2");
		key.setDeferrability(DatabaseMetaData.importedKeyInitiallyDeferred);
		key.setDeleteRule(DatabaseMetaData.importedKeyCascade);
		key.setUpdateRule(DatabaseMetaData.importedKeyRestrict);
		
		try
		{
			String result = this.getCreateForeignKeyConstraintSQL(key);
			
			assert result.equals("ALTER TABLE table ADD CONSTRAINT name FOREIGN KEY (column1, column2) REFERENCES foreign_table (foreign_column1, foreign_column2) ON DELETE CASCADE") : result;
		}
		catch (SQLException e)
		{
			assert false : e;
		}
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
			EasyMock.expect(statement.executeQuery("SELECT SEQUENCE_NAME FROM USER_SEQUENCES")).andReturn(resultSet);
			EasyMock.expect(resultSet.next()).andReturn(true);
			EasyMock.expect(resultSet.getString(1)).andReturn("sequence1");
			EasyMock.expect(resultSet.next()).andReturn(true);
			EasyMock.expect(resultSet.getString(1)).andReturn("sequence2");
			EasyMock.expect(resultSet.next()).andReturn(false);
			
			statement.close();
		
			EasyMock.replay(metaData, connection, statement, resultSet);
			
			Collection<QualifiedName> result = this.getSequences(metaData);
			
			EasyMock.verify(metaData, connection, statement, resultSet);
			
			assert result.size() == 2 : result;
			
			Iterator<QualifiedName> iterator = result.iterator();
			QualifiedName sequence = iterator.next();
			String schema = sequence.getSchema();
			String name = sequence.getName();
			
			assert (schema == null) : schema;
			assert name.equals("sequence1") : name;
			
			sequence = iterator.next();
			schema = sequence.getSchema();
			name = sequence.getName();
			
			assert (schema == null) : schema;
			assert name.equals("sequence2") : name;
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * @see net.sf.hajdbc.dialect.TestStandardDialect#testGetSimpleSQL()
	 */
	@Override
	public void testGetSimpleSQL()
	{
		try
		{
			String result = this.getSimpleSQL();
			
			assert result.equals("SELECT SYSDATE FROM DUAL") : result;
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * @see net.sf.hajdbc.dialect.TestStandardDialect#testGetTruncateTableSQL()
	 */
	@Override
	public void testGetTruncateTableSQL()
	{
		TableProperties table = EasyMock.createStrictMock(TableProperties.class);
		
		EasyMock.expect(table.getName()).andReturn("table");
		
		EasyMock.replay(table);
		
		try
		{
			String result = this.getTruncateTableSQL(table);

			EasyMock.verify(table);
			
			assert result.equals("TRUNCATE TABLE table") : result;
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	@Override
	@DataProvider(name = "sequence-sql")
	Object[][] sequenceSQLProvider()
	{
		return new Object[][] {
			new Object[] { "SELECT success.nextval" },
			new Object[] { "SELECT success.currval" },
			new Object[] { "SELECT success.nextval, * FROM table" },
			new Object[] { "SELECT success.currval, * FROM table" },
			new Object[] { "INSERT INTO table VALUES (success.nextval, 0)" },
			new Object[] { "INSERT INTO table VALUES (success.currval, 0)" },
			new Object[] { "UPDATE table SET id = success.nextval" },
			new Object[] { "UPDATE table SET id = success.currval" },
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
		try
		{
			String result = this.parseInsertTable(sql);
			
			assert (result == null) : result;
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
		
		try
		{
			String result = this.getNextSequenceValueSQL(sequence);

			EasyMock.verify(sequence);
			
			assert result.equals("SELECT sequence.NEXTVAL FROM DUAL") : result;
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}
}
