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
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;

import net.sf.hajdbc.Dialect;

import org.easymock.EasyMock;
import org.testng.annotations.Test;

/**
 * @author Paul Ferraro
 *
 */
public class TestDB2Dialect extends TestDefaultDialect
{
	private Statement statement = this.control.createMock(Statement.class);
	private ResultSet resultSet = this.control.createMock(ResultSet.class);
	
	@Override
	protected Dialect createDialect()
	{
		return new DB2Dialect();
	}

	/**
	 * @see net.sf.hajdbc.dialect.TestDefaultDialect#getSequences(java.sql.Connection)
	 */
	@Override
	@Test(dataProvider = "connection")
	public Map<String, Long> getSequences(Connection connection) throws SQLException
	{
		EasyMock.expect(connection.createStatement()).andReturn(this.statement);
		EasyMock.expect(this.statement.executeQuery("SELECT SEQNAME FROM SYSCAT.SEQUENCES")).andReturn(this.resultSet);
		EasyMock.expect(this.resultSet.next()).andReturn(true);
		EasyMock.expect(this.resultSet.getString(1)).andReturn("sequence1");
		EasyMock.expect(this.resultSet.next()).andReturn(true);
		EasyMock.expect(this.resultSet.getString(1)).andReturn("sequence2");
		EasyMock.expect(this.resultSet.next()).andReturn(false);
		
		this.resultSet.close();
		
		EasyMock.expect(this.statement.executeQuery("VALUES (PREVVAL FOR sequence2, PREVVAL FOR sequence1)")).andReturn(this.resultSet);
		EasyMock.expect(this.resultSet.next()).andReturn(true);
		EasyMock.expect(this.resultSet.getLong(1)).andReturn(2L);
		EasyMock.expect(this.resultSet.getLong(2)).andReturn(1L);
		
		this.resultSet.close();
		this.statement.close();
		
		this.control.replay();
		
		Map<String, Long> sequenceMap = this.dialect.getSequences(connection);
		
		this.control.verify();
		
		assert sequenceMap.size() == 2 : sequenceMap;
		assert sequenceMap.get("sequence1").equals(1L) : sequenceMap;
		assert sequenceMap.get("sequence2").equals(2L) : sequenceMap;
		
		return sequenceMap;
	}

	/**
	 * @see net.sf.hajdbc.dialect.TestDefaultDialect#getSimpleSQL()
	 */
	@Override
	public String getSimpleSQL()
	{
		this.control.replay();
		
		String sql = this.dialect.getSimpleSQL();
		
		this.control.verify();
		
		assert sql.equals("VALUES 1") : sql;
		
		return sql;
	}

	/**
	 * @see net.sf.hajdbc.dialect.TestDefaultDialect#parseSequence(java.lang.String)
	 */
	@Override
	@Test(dataProvider = "null")
	public String parseSequence(String sql) throws SQLException
	{
		this.control.replay();
		
		String sequence = this.dialect.parseSequence("VALUES NEXTVAL FOR sequence");
		
		this.control.verify();
		
		assert sequence.equals("sequence") : sequence;
		
		this.control.reset();
		this.control.replay();
		
		sequence = this.dialect.parseSequence("VALUES PREVVAL FOR sequence");
		
		this.control.verify();
		
		assert sequence.equals("sequence") : sequence;
		
		this.control.reset();
		this.control.replay();
		
		sequence = this.dialect.parseSequence("SELECT * FROM table");
		
		this.control.verify();
		
		assert sequence == null : sequence;
		
		return sequence;
	}
}
