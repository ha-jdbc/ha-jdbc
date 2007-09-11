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

import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Iterator;

import net.sf.hajdbc.Dialect;
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
public class TestFirebirdDialect extends TestStandardDialect
{
	/**
	 * @see net.sf.hajdbc.dialect.TestStandardDialect#createDialect()
	 */
	@Override
	protected Dialect createDialect()
	{
		return new FirebirdDialect();
	}
	
	/**
	 * @see net.sf.hajdbc.dialect.TestStandardDialect#getAlterSequenceSQL(java.lang.String, long)
	 */
	@Override
	@Test(dataProvider = "alter-sequence")
	public String getAlterSequenceSQL(SequenceProperties sequence, long value) throws SQLException
	{
		EasyMock.expect(sequence.getName()).andReturn("sequence");
		
		this.replay();
		
		String sql = this.dialect.getAlterSequenceSQL(sequence, value);
		
		this.verify();
		
		assert sql.equals("SET GENERATOR sequence TO 1") : sql;
		
		return sql;
	}

	/**
	 * @see net.sf.hajdbc.dialect.TestStandardDialect#getSequences(java.sql.Connection)
	 */
	@Override
	@Test(dataProvider = "meta-data")
	public Collection<QualifiedName> getSequences(DatabaseMetaData metaData) throws SQLException
	{
		EasyMock.expect(metaData.getConnection()).andReturn(this.connection);
		EasyMock.expect(this.connection.createStatement()).andReturn(this.statement);
		EasyMock.expect(this.statement.executeQuery("SELECT RDB$GENERATOR_NAME FROM RDB$GENERATORS")).andReturn(this.resultSet);
		EasyMock.expect(this.resultSet.next()).andReturn(true);
		EasyMock.expect(this.resultSet.getString(1)).andReturn("sequence1");
		EasyMock.expect(this.resultSet.next()).andReturn(true);
		EasyMock.expect(this.resultSet.getString(1)).andReturn("sequence2");
		EasyMock.expect(this.resultSet.next()).andReturn(false);
		
		this.statement.close();
		
		this.replay();
		
		Collection<QualifiedName> sequences = this.dialect.getSequences(metaData);
		
		this.verify();
		
		assert sequences.size() == 2 : sequences;
		
		Iterator<QualifiedName> iterator = sequences.iterator();
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
		
		return sequences;
	}
	
	/**
	 * @see net.sf.hajdbc.Dialect#getCurrentSequenceValueSQL(java.lang.String)
	 */
	@Override
	@Test(dataProvider = "sequence")
	public String getNextSequenceValueSQL(SequenceProperties sequence) throws SQLException
	{
		EasyMock.expect(sequence.getName()).andReturn("sequence");
		
		this.replay();
		
		String sql = this.dialect.getNextSequenceValueSQL(sequence);
		
		this.verify();
		
		assert sql.equals("SELECT GEN_ID(sequence, 1) FROM RDB$DATABASE") : sql;
		
		return sql;
	}

	/**
	 * @see net.sf.hajdbc.dialect.TestStandardDialect#getSimpleSQL()
	 */
	@Override
	@Test
	public String getSimpleSQL() throws SQLException
	{
		String sql = this.dialect.getSimpleSQL();
		
		assert sql.equals("SELECT CURRENT_TIMESTAMP FROM RDB$DATABASE") : sql;
		
		return sql;
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
	 * @see net.sf.hajdbc.Dialect#parseInsertTable(java.lang.String)
	 */
	@Override
	@Test(dataProvider = "insert-table-sql")
	public String parseInsertTable(String sql) throws SQLException
	{
		this.replay();
		
		String table = this.dialect.parseInsertTable(sql);
		
		this.verify();

		assert (table == null) : table;

		return table;
	}
}
