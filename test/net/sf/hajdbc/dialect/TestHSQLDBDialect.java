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
import net.sf.hajdbc.ForeignKeyConstraint;

import org.easymock.EasyMock;
import org.testng.annotations.Test;

/**
 * @author Paul Ferraro
 *
 */
public class TestHSQLDBDialect extends TestDefaultDialect
{
	private Statement statement = this.control.createMock(Statement.class);
	private ResultSet resultSet = this.control.createMock(ResultSet.class);
	
	@Override
	protected Dialect createDialect()
	{
		return new HSQLDBDialect();
	}

	/**
	 * @see net.sf.hajdbc.dialect.TestDefaultDialect#getCreateForeignKeyConstraintSQL(net.sf.hajdbc.ForeignKeyConstraint)
	 */
	@Override
	@Test(dataProvider = "foreign-key")
	public String getCreateForeignKeyConstraintSQL(ForeignKeyConstraint constraint) throws SQLException
	{
		this.control.replay();
		
		String sql = this.dialect.getCreateForeignKeyConstraintSQL(constraint);
		
		assert sql.equals("ALTER TABLE table ADD CONSTRAINT name FOREIGN KEY (column1, column2) REFERENCES foreign_table (foreign_column1, foreign_column2) ON DELETE CASCADE ON UPDATE RESTRICT") : sql;
		
		return sql;
	}

	/**
	 * @see net.sf.hajdbc.dialect.TestDefaultDialect#getSequences(java.sql.Connection)
	 */
	@Override
	@Test(dataProvider = "connection")
	public Map<String, Long> getSequences(Connection connection) throws SQLException
	{
		EasyMock.expect(connection.createStatement()).andReturn(this.statement);
		EasyMock.expect(this.statement.executeQuery("SELECT SEQUENCE_SCHEMA, SEQUENCE_NAME, NEXT_VALUE FROM INFORMATION_SCHEMA.SYSTEM_SEQUENCES")).andReturn(this.resultSet);
		EasyMock.expect(this.resultSet.next()).andReturn(true);
		EasyMock.expect(this.resultSet.getString(1)).andReturn("schema");
		EasyMock.expect(this.resultSet.getString(2)).andReturn("sequence1");
		EasyMock.expect(this.resultSet.getLong(3)).andReturn(1L);
		EasyMock.expect(this.resultSet.next()).andReturn(true);
		EasyMock.expect(this.resultSet.getString(1)).andReturn("schema");
		EasyMock.expect(this.resultSet.getString(2)).andReturn("sequence2");
		EasyMock.expect(this.resultSet.getLong(3)).andReturn(2L);
		EasyMock.expect(this.resultSet.next()).andReturn(false);
		EasyMock.expect(this.resultSet.getStatement()).andReturn(this.statement);
		
		this.statement.close();
		
		this.control.replay();
		
		Map<String, Long> sequenceMap = this.dialect.getSequences(connection);
		
		this.control.verify();
		
		assert sequenceMap.size() == 2 : sequenceMap;
		assert sequenceMap.get("schema.sequence1").equals(1L) : sequenceMap;
		assert sequenceMap.get("schema.sequence2").equals(2L) : sequenceMap;
		
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
		
		assert sql.equals("CALL NOW()") : sql;
		
		return sql;
	}
}
