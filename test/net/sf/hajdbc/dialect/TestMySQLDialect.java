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
import net.sf.hajdbc.UniqueConstraint;

/**
 * @author Paul Ferraro
 *
 */
public class TestMySQLDialect extends TestStandardDialect
{
	/**
	 * @see net.sf.hajdbc.dialect.TestStandardDialect#createDialect()
	 */
	@Override
	protected Dialect createDialect()
	{
		return new MySQLDialect();
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
	 * @see net.sf.hajdbc.dialect.TestStandardDialect#getCreateForeignKeyConstraintSQL(net.sf.hajdbc.ForeignKeyConstraint)
	 */
	@Override
	@Test(dataProvider = "foreign-key")
	public String getDropForeignKeyConstraintSQL(ForeignKeyConstraint constraint) throws SQLException
	{
		this.control.replay();
		
		String sql = this.dialect.getDropForeignKeyConstraintSQL(constraint);
		
		assert sql.equals("ALTER TABLE table DROP FOREIGN KEY name") : sql;
		
		return sql;
	}

	/**
	 * @see net.sf.hajdbc.dialect.TestStandardDialect#getCreateForeignKeyConstraintSQL(net.sf.hajdbc.ForeignKeyConstraint)
	 */
	@Override
	@Test(dataProvider = "unique-constraint")
	public String getCreateUniqueConstraintSQL(UniqueConstraint constraint) throws SQLException
	{
		this.control.replay();
		
		String sql = this.dialect.getCreateUniqueConstraintSQL(constraint);
		
		assert sql.equals("ALTER TABLE table ADD UNIQUE name (column1, column2)") : sql;
		
		return sql;
	}

	/**
	 * @see net.sf.hajdbc.dialect.TestStandardDialect#getCreateForeignKeyConstraintSQL(net.sf.hajdbc.ForeignKeyConstraint)
	 */
	@Override
	@Test(dataProvider = "unique-constraint")
	public String getDropUniqueConstraintSQL(UniqueConstraint constraint) throws SQLException
	{
		this.control.replay();
		
		String sql = this.dialect.getDropUniqueConstraintSQL(constraint);
		
		assert sql.equals("ALTER TABLE table DROP INDEX name") : sql;
		
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
	 * @see net.sf.hajdbc.dialect.TestStandardDialect#getDefaultSchemas(java.sql.Connection)
	 */
	@Override
	public List<String> getDefaultSchemas(Connection connection) throws SQLException
	{
		EasyMock.expect(connection.createStatement()).andReturn(this.statement);
		EasyMock.expect(this.statement.executeQuery("SELECT DATABASE()")).andReturn(this.resultSet);
		EasyMock.expect(this.resultSet.next()).andReturn(false);
		EasyMock.expect(this.resultSet.getString(1)).andReturn("database");

		this.resultSet.close();
		this.statement.close();
		
		this.control.replay();
		
		List<String> schemaList = this.dialect.getDefaultSchemas(connection);
		
		this.control.verify();
		
		assert schemaList.size() == 1 : schemaList.size();
		
		assert schemaList.get(0).equals("database") : schemaList.get(0);
		
		return schemaList;
	}

	/**
	 * @see net.sf.hajdbc.dialect.TestStandardDialect#supportsSequences()
	 */
	@Override
	public boolean supportsSequences()
	{
		this.control.replay();
		
		boolean supports = this.dialect.supportsSequences();
		
		this.control.verify();
		
		assert !supports;
		
		return supports;
	}

	/**
	 * @see net.sf.hajdbc.dialect.TestStandardDialect#isIdentity(net.sf.hajdbc.ColumnProperties)
	 */
	@Override
	public boolean isIdentity(ColumnProperties properties) throws SQLException
	{
		EasyMock.expect(properties.getRemarks()).andReturn("AUTO_INCREMENT");
		
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
}
