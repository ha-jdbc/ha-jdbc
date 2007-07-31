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
import java.sql.SQLException;
import java.sql.Types;
import java.util.List;
import java.util.regex.Pattern;

import net.sf.hajdbc.ColumnProperties;
import net.sf.hajdbc.Dialect;
import net.sf.hajdbc.TableProperties;

import org.easymock.EasyMock;
import org.testng.annotations.Test;

/**
 * @author Paul Ferraro
 *
 */
public class TestPostgreSQLDialect extends TestStandardDialect
{
	@Override
	protected Dialect createDialect()
	{
		return new PostgreSQLDialect();
	}

	/**
	 * @see net.sf.hajdbc.dialect.TestStandardDialect#getColumnType(net.sf.hajdbc.ColumnProperties)
	 */
	@Override
	@Test(dataProvider = "column")
	public int getColumnType(ColumnProperties properties) throws SQLException
	{
		EasyMock.expect(properties.getNativeType()).andReturn("oid");
		
		this.replay();
		
		int type = this.dialect.getColumnType(properties);
		
		this.verify();
		
		assert type == Types.BLOB : type;
		
		EasyMock.expect(properties.getNativeType()).andReturn("int");
		
		return super.getColumnType(properties);
	}

	/**
	 * @see net.sf.hajdbc.dialect.TestStandardDialect#getLockTableSQL(net.sf.hajdbc.TableProperties)
	 */
	@Override
	@Test(dataProvider = "table")
	public String getLockTableSQL(TableProperties properties) throws SQLException
	{
		EasyMock.expect(properties.getName()).andReturn("table");
		
		this.replay();
		
		String sql = this.dialect.getLockTableSQL(properties);
		
		this.verify();
		
		assert sql.equals("LOCK TABLE table IN EXCLUSIVE MODE") : sql;
		
		return sql;
	}

	/**
	 * @see net.sf.hajdbc.dialect.TestStandardDialect#getTruncateTableSQL(net.sf.hajdbc.TableProperties)
	 */
	@Override
	@Test(dataProvider = "table")
	public String getTruncateTableSQL(TableProperties properties) throws SQLException
	{
		EasyMock.expect(properties.getName()).andReturn("table");
		
		this.replay();
		
		String sql = this.dialect.getTruncateTableSQL(properties);
		
		this.verify();
		
		assert sql.equals("TRUNCATE TABLE table");
		
		return sql;
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
		
		assert sql.equals("SELECT NEXTVAL('sequence')") : sql;
		
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
		
		String sequence = this.dialect.parseSequence("SELECT CURRVAL('sequence')");
		
		this.verify();
		
		assert sequence.equals("sequence") : sequence;
		
		this.replay();
		
		sequence = this.dialect.parseSequence("SELECT nextval('sequence')");
		
		this.verify();
		
		assert sequence.equals("sequence") : sequence;
		
		this.replay();
		
		sequence = this.dialect.parseSequence("SELECT * FROM table");
		
		this.verify();
		
		assert sequence == null : sequence;
		
		return sequence;
	}

	/**
	 * @see net.sf.hajdbc.dialect.TestStandardDialect#getDefaultSchemas(java.sql.Connection)
	 */
	@Override
	public List<String> getDefaultSchemas(Connection connection) throws SQLException
	{
		EasyMock.expect(connection.createStatement()).andReturn(this.statement);
		EasyMock.expect(this.statement.executeQuery("SHOW search_path")).andReturn(this.resultSet);
		EasyMock.expect(this.resultSet.next()).andReturn(false);
		EasyMock.expect(this.resultSet.getString(1)).andReturn("$user,public");

		this.resultSet.close();
		this.statement.close();
		
		EasyMock.expect(connection.createStatement()).andReturn(this.statement);
		EasyMock.expect(this.statement.executeQuery("SELECT CURRENT_USER")).andReturn(this.resultSet);
		EasyMock.expect(this.resultSet.next()).andReturn(false);
		EasyMock.expect(this.resultSet.getString(1)).andReturn("user");

		this.resultSet.close();
		this.statement.close();
		
		this.replay();
		
		List<String> schemaList = this.dialect.getDefaultSchemas(connection);
		
		this.verify();
		
		assert schemaList.size() == 2 : schemaList.size();
		
		assert schemaList.get(0).equals("user") : schemaList.get(0);
		assert schemaList.get(1).equals("public") : schemaList.get(1);
		
		return schemaList;
	}

	/**
	 * @see net.sf.hajdbc.dialect.TestStandardDialect#isIdentity(net.sf.hajdbc.ColumnProperties)
	 */
	@Override
	public boolean isIdentity(ColumnProperties properties) throws SQLException
	{
		EasyMock.expect(properties.getNativeType()).andReturn("serial");
		
		this.replay();
		
		boolean identity = this.dialect.isIdentity(properties);
		
		this.verify();
		
		assert identity;
		
		EasyMock.expect(properties.getNativeType()).andReturn("bigserial");
		
		this.replay();
		
		identity = this.dialect.isIdentity(properties);
		
		this.verify();
		
		assert identity;
		
		EasyMock.expect(this.columnProperties.getNativeType()).andReturn("int");
		
		this.replay();
		
		identity = this.dialect.isIdentity(properties);
		
		this.verify();

		assert !identity;
		
		return identity;
	}
	
	/**
	 * @see net.sf.hajdbc.Dialect#getIdentifierPattern(java.sql.DatabaseMetaData)
	 */
	@Override
	@Test(dataProvider = "meta-data")
	public Pattern getIdentifierPattern(DatabaseMetaData metaData) throws SQLException
	{
		EasyMock.expect(metaData.getDriverMajorVersion()).andReturn(8);
		EasyMock.expect(metaData.getDriverMinorVersion()).andReturn(0);
		EasyMock.expect(metaData.getExtraNameCharacters()).andReturn("");
		
		this.replay();
		
		Pattern pattern = this.dialect.getIdentifierPattern(metaData);
		
		this.verify();
		
		assert pattern.pattern().equals("[\\w]+");
		
		EasyMock.expect(metaData.getDriverMajorVersion()).andReturn(8);
		EasyMock.expect(metaData.getDriverMinorVersion()).andReturn(1);
		
		this.replay();
		
		pattern = this.dialect.getIdentifierPattern(metaData);
		
		this.verify();
		
		assert pattern.pattern().equals("[A-Za-z\\0200-\\0377_][A-Za-z\\0200-\\0377_0-9\\$]*");
		
		return pattern;
	}
}
