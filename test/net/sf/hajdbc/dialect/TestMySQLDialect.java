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
		this.replay();
		
		String sql = this.dialect.getCreateForeignKeyConstraintSQL(constraint);
		
		this.verify();
		
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
		this.replay();
		
		String sql = this.dialect.getDropForeignKeyConstraintSQL(constraint);
		
		this.verify();
		
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
		this.replay();
		
		String sql = this.dialect.getCreateUniqueConstraintSQL(constraint);
		
		this.verify();
		
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
		this.replay();
		
		String sql = this.dialect.getDropUniqueConstraintSQL(constraint);
		
		this.verify();
		
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
		this.replay();
		
		String sequence = this.dialect.parseSequence("SELECT NEXT VALUE FROM sequence");
		
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
		EasyMock.expect(this.statement.executeQuery("SELECT DATABASE()")).andReturn(this.resultSet);
		EasyMock.expect(this.resultSet.next()).andReturn(false);
		EasyMock.expect(this.resultSet.getString(1)).andReturn("database");

		this.resultSet.close();
		this.statement.close();
		
		this.replay();
		
		List<String> schemaList = this.dialect.getDefaultSchemas(connection);
		
		this.verify();
		
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
		this.replay();
		
		boolean supports = this.dialect.supportsSequences();
		
		this.verify();
		
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
		
		this.replay();
		
		boolean identity = this.dialect.isIdentity(properties);
		
		this.verify();
		
		EasyMock.expect(this.columnProperties.getRemarks()).andReturn(null);
		
		this.replay();
		
		identity = this.dialect.isIdentity(properties);
		
		this.verify();
		
		return identity;
	}

	/**
	 * @see net.sf.hajdbc.dialect.TestStandardDialect#supportsIdentityColumns()
	 */
	@Override
	public boolean supportsIdentityColumns()
	{
		this.replay();
		
		boolean supports = this.dialect.supportsIdentityColumns();
		
		this.verify();
		
		assert !supports;
		
		return supports;
	}
}
