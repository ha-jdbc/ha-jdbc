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
import java.util.List;

import org.easymock.EasyMock;
import org.testng.annotations.Test;

import net.sf.hajdbc.ColumnProperties;
import net.sf.hajdbc.Dialect;
import net.sf.hajdbc.ForeignKeyConstraint;
import net.sf.hajdbc.TableProperties;

/**
 * @author Paul Ferraro
 *
 */
public class TestDerbyDialect extends TestStandardDialect
{
	@Override
	protected Dialect createDialect()
	{
		return new DerbyDialect();
	}

	/**
	 * @see net.sf.hajdbc.dialect.TestStandardDialect#getCreateForeignKeyConstraintSQL(net.sf.hajdbc.ForeignKeyConstraint)
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
	 * @see net.sf.hajdbc.dialect.TestStandardDialect#getLockTableSQL(net.sf.hajdbc.TableProperties)
	 */
	@Override
	@Test(dataProvider = "table")
	public String getLockTableSQL(TableProperties properties) throws SQLException
	{
		EasyMock.expect(properties.getName()).andReturn("table");
		
		this.control.replay();
		
		String sql = this.dialect.getLockTableSQL(properties);
		
		assert sql.equals("LOCK TABLE table IN SHARE MODE") : sql;
		
		return sql;
	}

	/**
	 * @see net.sf.hajdbc.dialect.TestStandardDialect#getSimpleSQL()
	 */
	@Override
	@Test
	public String getSimpleSQL() throws SQLException
	{
		this.control.replay();
		
		String sql = this.dialect.getSimpleSQL();
		
		this.control.verify();
		
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
		this.control.replay();
		
		String sequence = this.dialect.parseSequence("SELECT NEXT VALUE FROM sequence");
		
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
		
		this.control.replay();
		
		List<String> schemaList = this.dialect.getDefaultSchemas(connection);
		
		this.control.verify();
		
		assert schemaList.size() == 1 : schemaList.size();
		
		assert schemaList.get(0).equals("user") : schemaList.get(0);
		
		return schemaList;
	}

	/**
	 * @see net.sf.hajdbc.Dialect#isIdentity(net.sf.hajdbc.ColumnProperties)
	 */
	@Override
	@Test(dataProvider = "column")
	public boolean isIdentity(ColumnProperties properties) throws SQLException
	{
		EasyMock.expect(properties.getRemarks()).andReturn("GENERATED ALWAYS AS IDENTITY");
		
		this.control.replay();
		
		boolean identity = this.dialect.isIdentity(properties);
		
		this.control.verify();
		this.control.reset();
		
		EasyMock.expect(this.columnProperties.getRemarks()).andReturn(null);
		
		this.control.replay();
		
		identity = this.dialect.isIdentity(properties);
		
		this.control.verify();
		
		return identity;
	}
	
	/**
	 * @see net.sf.hajdbc.Dialect#getCurrentSequenceValueSQL(java.lang.String)
	 */
	@Override
	@Test(dataProvider = "sequence")
	public String getNextSequenceValueSQL(String sequence) throws SQLException
	{
		this.control.replay();
		
		String sql = this.dialect.getNextSequenceValueSQL(sequence);
		
		this.control.verify();
		
		assert sql.equals("VALUES NEXT VALUE FOR sequence") : sql;
		
		return sql;
	}

	/**
	 * @see net.sf.hajdbc.Dialect#supportsSequences()
	 */
	@Override
	@Test
	public boolean supportsSequences()
	{
		this.control.replay();
		
		boolean supports = this.dialect.supportsSequences();
		
		this.control.verify();
		
		assert !supports;
		
		return supports;
	}
}
