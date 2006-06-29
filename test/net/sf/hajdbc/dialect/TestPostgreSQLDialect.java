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
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.Map;

import net.sf.hajdbc.ColumnProperties;
import net.sf.hajdbc.Dialect;
import net.sf.hajdbc.TableProperties;

import org.easymock.EasyMock;
import org.testng.annotations.Test;

/**
 * @author Paul Ferraro
 *
 */
@Test
public class TestPostgreSQLDialect extends TestDefaultDialect
{
	private DatabaseMetaData metaData = this.control.createMock(DatabaseMetaData.class);
	private Statement statement = this.control.createMock(Statement.class);
	private ResultSet resultSet = this.control.createMock(ResultSet.class);
	
	@Override
	protected Dialect createDialect()
	{
		return new PostgreSQLDialect();
	}

	/**
	 * @see net.sf.hajdbc.dialect.TestDefaultDialect#getColumnType(net.sf.hajdbc.ColumnProperties)
	 */
	@Override
	@Test(dataProvider = "column")
	public int getColumnType(ColumnProperties properties) throws SQLException
	{
		ColumnProperties oidProperties = new ColumnProperties("column", Types.INTEGER, "oid");
		
		this.control.replay();
		
		int type = this.dialect.getColumnType(oidProperties);
		
		this.control.verify();
		
		assert type == Types.BLOB : type;
		
		this.control.reset();
		
		return super.getColumnType(properties);
	}

	/**
	 * @see net.sf.hajdbc.dialect.TestDefaultDialect#getLockTableSQL(net.sf.hajdbc.TableProperties)
	 */
	@Override
	public String getLockTableSQL(TableProperties properties) throws SQLException
	{
		EasyMock.expect(properties.getName()).andReturn("table");
		
		this.control.replay();
		
		String sql = this.dialect.getLockTableSQL(properties);
		
		this.control.verify();
		
		assert sql.equals("LOCK TABLE table IN EXCLUSIVE MODE; SELECT 1 FROM table") : sql;
		
		return sql;
	}

	/**
	 * @see net.sf.hajdbc.dialect.TestDefaultDialect#getSequences(java.sql.Connection)
	 */
	@Override
	@Test(dataProvider = "connection")
	public Map<String, Long> getSequences(Connection connection) throws SQLException
	{
		EasyMock.expect(connection.getMetaData()).andReturn(this.metaData);
		EasyMock.expect(connection.getCatalog()).andReturn(null);
		EasyMock.expect(this.metaData.getTables("", null, "%", new String[] { "SEQUENCE" })).andReturn(this.resultSet);
		EasyMock.expect(this.resultSet.next()).andReturn(true);
		EasyMock.expect(this.resultSet.getString("TABLE_SCHEM")).andReturn("schema");
		EasyMock.expect(this.resultSet.getString("TABLE_NAME")).andReturn("sequence1");
		EasyMock.expect(this.resultSet.next()).andReturn(true);
		EasyMock.expect(this.resultSet.getString("TABLE_SCHEM")).andReturn("schema");
		EasyMock.expect(this.resultSet.getString("TABLE_NAME")).andReturn("sequence1");
		EasyMock.expect(this.resultSet.next()).andReturn(false);
		
		this.resultSet.close();
		
		EasyMock.expect(this.statement.executeQuery("SELECT CURRVAL('schema.sequence2'), CURRVAL('schema.sequence1')")).andReturn(this.resultSet);
		EasyMock.expect(this.resultSet.next()).andReturn(true);
		EasyMock.expect(this.resultSet.getLong(1)).andReturn(2L);
		EasyMock.expect(this.resultSet.getLong(2)).andReturn(1L);
		
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
	 * @see net.sf.hajdbc.dialect.TestDefaultDialect#getTruncateTableSQL(net.sf.hajdbc.TableProperties)
	 */
	@Override
	public String getTruncateTableSQL(TableProperties properties) throws SQLException
	{
		EasyMock.expect(properties.getName()).andReturn("table");
		
		this.control.replay();
		
		String sql = this.dialect.getTruncateTableSQL(properties);
		
		this.control.verify();
		
		assert sql.equals("TRUNCATE TABLE table");
		
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
		
		String sequence = this.dialect.parseSequence("SELECT currval('sequence')");
		
		this.control.verify();
		
		assert sequence.equals("sequence") : sequence;
		
		this.control.reset();
		this.control.replay();
		
		sequence = this.dialect.parseSequence("SELECT nextval('sequence')");
		
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
