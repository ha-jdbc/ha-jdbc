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
public class TestDB2Dialect extends TestStandardDialect
{
	@Override
	protected Dialect createDialect()
	{
		return new DB2Dialect();
	}

	/**
	 * @see net.sf.hajdbc.dialect.TestStandardDialect#getSequences(java.sql.Connection)
	 */
	@Override
	@Test(dataProvider = "connection")
	public Collection<String> getSequences(Connection connection) throws SQLException
	{
		EasyMock.expect(connection.createStatement()).andReturn(this.statement);
		EasyMock.expect(this.statement.executeQuery("SELECT SEQNAME FROM SYSCAT.SEQUENCES")).andReturn(this.resultSet);
		EasyMock.expect(this.resultSet.next()).andReturn(true);
		EasyMock.expect(this.resultSet.getString(1)).andReturn("sequence1");
		EasyMock.expect(this.resultSet.next()).andReturn(true);
		EasyMock.expect(this.resultSet.getString(1)).andReturn("sequence2");
		EasyMock.expect(this.resultSet.next()).andReturn(false);
		
		this.resultSet.close();
		this.statement.close();
		
		this.replay();
		
		Collection<String> sequences = this.dialect.getSequences(connection);
		
		this.verify();

		assert sequences.size() == 2 : sequences.size();
		
		Iterator<String> iterator = sequences.iterator();
		String sequence = iterator.next();
		
		assert sequence.equals("sequence1") : sequence;
		
		sequence = iterator.next();
		
		assert sequence.equals("sequence2") : sequence;
		
		return sequences;
	}

	/**
	 * @see net.sf.hajdbc.dialect.TestStandardDialect#getCurrentSequenceValueSQL(java.lang.String)
	 */
	@Override
	@Test(dataProvider = "sequence")
	public String getNextSequenceValueSQL(String sequence) throws SQLException
	{
		this.replay();
		
		String sql = this.dialect.getNextSequenceValueSQL(sequence);
		
		this.verify();
		
		assert sql.equals("VALUES NEXTVAL FOR sequence") : sql;
		
		return sql;
	}

	/**
	 * @see net.sf.hajdbc.dialect.TestStandardDialect#getSimpleSQL()
	 */
	@Override
	public String getSimpleSQL() throws SQLException
	{
		this.replay();
		
		String sql = this.dialect.getSimpleSQL();

		this.verify();
		
		assert sql.equals("VALUES CURRENT_TIMESTAMP") : sql;
		
		return sql;
	}

	/**
	 * @see net.sf.hajdbc.dialect.TestStandardDialect#parseSequence(java.lang.String)
	 */
	@Override
	@Test(dataProvider = "null")
	public String parseSequence(String sql) throws SQLException
	{
		this.replay();
		
		String sequence = this.dialect.parseSequence("VALUES NEXTVAL FOR sequence");
		
		this.verify();
		
		assert sequence.equals("sequence") : sequence;
		
		this.replay();
		
		sequence = this.dialect.parseSequence("VALUES PREVVAL FOR sequence");
		
		this.verify();
		
		assert sequence.equals("sequence") : sequence;
		
		this.replay();
		
		sequence = this.dialect.parseSequence("SELECT * FROM table");
		
		this.verify();
		
		assert sequence == null : sequence;
		
		return sequence;
	}

	/**
	 * @see net.sf.hajdbc.Dialect#getDefaultSchemas(java.sql.Connection)
	 */
	@Override
	@Test(dataProvider = "connection")
	public List<String> getDefaultSchemas(Connection connection) throws SQLException
	{
		EasyMock.expect(connection.createStatement()).andReturn(this.statement);
		EasyMock.expect(this.statement.executeQuery("VALUES CURRENT_USER")).andReturn(this.resultSet);
		EasyMock.expect(this.resultSet.next()).andReturn(false);
		EasyMock.expect(this.resultSet.getString(1)).andReturn("user");

		this.resultSet.close();
		this.statement.close();
		
		this.replay();
		
		List<String> schemaList = this.dialect.getDefaultSchemas(connection);
		
		this.verify();
		
		assert schemaList.size() == 1 : schemaList.size();
		
		assert schemaList.get(0).equals("user") : schemaList.get(0);
		
		return schemaList;
	}
}
