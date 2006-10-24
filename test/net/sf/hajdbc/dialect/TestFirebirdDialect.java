/*
 * HA-JDBC: High-Availability JDBC
 * Copyright (c) 2004-2006 Paul Ferraro
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
import java.sql.SQLException;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import net.sf.hajdbc.Dialect;

import org.easymock.EasyMock;
import org.testng.annotations.Test;

/**
 * @author Paul Ferraro
 *
 */
@Test
public class TestFirebirdDialect extends TestDefaultDialect
{
	/**
	 * @see net.sf.hajdbc.dialect.TestDefaultDialect#createDialect()
	 */
	@Override
	protected Dialect createDialect()
	{
		return new FirebirdDialect();
	}
	
	/**
	 * @see net.sf.hajdbc.dialect.TestDefaultDialect#getAlterSequenceSQL(java.lang.String, long)
	 */
	@Override
	@Test(dataProvider = "alter-sequence")
	public String getAlterSequenceSQL(String sequence, long value) throws SQLException
	{
		this.control.replay();
		
		String sql = this.dialect.getAlterSequenceSQL(sequence, value);
		
		this.control.verify();
		
		assert sql.equals("SET GENERATOR sequence TO 1") : sql;
		
		return sql;
	}

	/**
	 * @see net.sf.hajdbc.dialect.TestDefaultDialect#getSequences(java.sql.Connection)
	 */
	@Override
	@Test(dataProvider = "connection")
	public Collection<String> getSequences(Connection connection) throws SQLException
	{
		EasyMock.expect(connection.createStatement()).andReturn(this.statement);
		EasyMock.expect(this.statement.executeQuery("SELECT RDB$GENERATOR_NAME FROM RDB$GENERATORS")).andReturn(this.resultSet);
		EasyMock.expect(this.resultSet.next()).andReturn(true);
		EasyMock.expect(this.resultSet.getString(1)).andReturn("sequence1");
		EasyMock.expect(this.resultSet.next()).andReturn(true);
		EasyMock.expect(this.resultSet.getString(1)).andReturn("sequence2");
		EasyMock.expect(this.resultSet.next()).andReturn(false);
		
		this.resultSet.close();
		this.statement.close();
		
		this.control.replay();
		
		Collection<String> sequences = this.dialect.getSequences(connection);
		
		this.control.verify();
		
		assert sequences.size() == 2 : sequences;
		
		Iterator<String> iterator = sequences.iterator();
		String sequence = iterator.next();
		
		assert sequence.equals("sequence1") : sequence;

		sequence = iterator.next();
		
		assert sequence.equals("sequence2") : sequence;
		
		return sequences;
	}
	
	/**
	 * @see net.sf.hajdbc.Dialect#getCurrentSequenceValueSQL(java.lang.String)
	 */
	@Override
	@Test(dataProvider = "sequence")
	public String getCurrentSequenceValueSQL(String sequence) throws SQLException
	{
		this.control.replay();
		
		String sql = this.dialect.getCurrentSequenceValueSQL(sequence);
		
		this.control.verify();
		
		assert sql.equals("SELECT GEN_ID(sequence, 0) FROM RDB$DATABASE") : sql;
		
		return sql;
	}

	/**
	 * @see net.sf.hajdbc.dialect.TestDefaultDialect#getSimpleSQL()
	 */
	@Override
	@Test
	public String getSimpleSQL() throws SQLException
	{
		String sql = this.dialect.getSimpleSQL();
		
		assert sql.equals("SELECT CURRENT_TIMESTAMP FROM RDB$DATABASE") : sql;
		
		return sql;
	}

	/**
	 * @see net.sf.hajdbc.dialect.TestDefaultDialect#isSelectForUpdate(java.lang.String)
	 */
	@Override
	@Test(dataProvider = "null")
	public boolean isSelectForUpdate(String sql) throws SQLException
	{
		this.control.replay();
		
		boolean selectForUpdate = this.dialect.isSelectForUpdate("SELECT * FROM table FOR UPDATE");
		
		this.control.verify();
		
		assert !selectForUpdate;
		
		this.control.reset();
		this.control.replay();
		
		selectForUpdate = this.dialect.isSelectForUpdate("SELECT * FROM table FOR UPDATE WITH LOCK");
		
		this.control.verify();
		
		assert selectForUpdate;
		
		return selectForUpdate;
	}

	/**
	 * @see net.sf.hajdbc.dialect.TestDefaultDialect#parseSequence(java.lang.String)
	 */
	@Override
	@Test(dataProvider = "null")
	public String parseSequence(String sql) throws SQLException
	{
		this.control.replay();
		
		String sequence = this.dialect.parseSequence("SELECT GEN_ID(sequence, 1) FROM RDB$DATABASE");
		
		this.control.verify();
		
		assert sequence.equals("sequence") : sequence;
		
		this.control.reset();
		this.control.replay();
		
		sequence = this.dialect.parseSequence("SELECT * FROM RDB$DATABASE");
		
		this.control.verify();
		
		assert sequence == null : sequence;
		
		return sequence;
	}

	/**
	 * @see net.sf.hajdbc.Dialect#supportsAutoIncrementColumns()
	 */
	@Test
	@Override
	public boolean supportsAutoIncrementColumns()
	{
		this.control.replay();
		
		boolean supports = this.dialect.supportsAutoIncrementColumns();
		
		this.control.verify();
		
		assert !supports;
		
		return supports;
	}

	/**
	 * @see net.sf.hajdbc.Dialect#getDefaultSchemas(java.sql.Connection)
	 */
	@Override
	@Test(dataProvider = "connection")
	public List<String> getDefaultSchemas(Connection connection) throws SQLException
	{
		EasyMock.expect(connection.createStatement()).andReturn(this.statement);
		EasyMock.expect(this.statement.executeQuery("SELECT CURRENT_USER FROM RDB$DATABASE")).andReturn(this.resultSet);
		EasyMock.expect(this.resultSet.next()).andReturn(false);
		EasyMock.expect(this.resultSet.getString(1)).andReturn("user");

		this.resultSet.close();
		this.statement.close();
		
		this.control.replay();
		
		List<String> schemaList = this.dialect.getDefaultSchemas(connection);
		
		this.control.verify();
		
		assert schemaList.size() == 1 : schemaList.size();
		
		assert schemaList.get(0).equals("user") : schemaList.get(0);
		
		return schemaList;
	}
}
