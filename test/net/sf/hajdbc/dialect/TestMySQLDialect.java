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

import java.sql.SQLException;

import net.sf.hajdbc.Dialect;
import net.sf.hajdbc.ForeignKeyConstraint;
import net.sf.hajdbc.UniqueConstraint;

import org.easymock.EasyMock;
import org.testng.annotations.Test;

/**
 * @author Paul Ferraro
 *
 */
@Test
public class TestMySQLDialect extends TestDefaultDialect
{
	/**
	 * @see net.sf.hajdbc.dialect.TestDefaultDialect#createDialect()
	 */
	@Override
	protected Dialect createDialect()
	{
		return new MySQLDialect();
	}

	/**
	 * Test case for {@link net.sf.hajdbc.Dialect#getDropForeignKeyConstraintSQL(java.sql.DatabaseMetaData, java.lang.String, java.lang.String, java.lang.String)}
	 */
	@Override
	public void testGetDropForeignKeyConstraintSQL()
	{
		ForeignKeyConstraint constraint = new ForeignKeyConstraint("fk_name", "schema", "table");
		String quote = "'";
		
		try
		{
			EasyMock.expect(this.metaData.supportsSchemasInDataManipulation()).andReturn(true);
			EasyMock.expect(this.metaData.getIdentifierQuoteString()).andReturn(quote).times(2);
			
			this.control.replay();
			
			String sql = this.dialect.getDropForeignKeyConstraintSQL(this.metaData, constraint);
			
			this.control.verify();
			
			assert sql.equals("ALTER TABLE 'schema'.'table' DROP FOREIGN KEY fk_name") : sql;
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test case for {@link net.sf.hajdbc.Dialect#getCreateUniqueConstraintSQL(java.sql.DatabaseMetaData, java.lang.String, java.lang.String, java.lang.String, java.util.List)}
	 */
	@Override
	public void testGetCreateUniqueConstraintSQL()
	{
		UniqueConstraint constraint = new UniqueConstraint("uk_name", "schema", "table");
		constraint.getColumnList().add("column1");
		constraint.getColumnList().add("column2");
		String quote = "'";
		
		try
		{
			EasyMock.expect(this.metaData.getIdentifierQuoteString()).andReturn(quote).times(2);
			EasyMock.expect(this.metaData.supportsSchemasInDataManipulation()).andReturn(true);
			EasyMock.expect(this.metaData.getIdentifierQuoteString()).andReturn(quote).times(2);
			
			this.control.replay();
			
			String sql = this.dialect.getCreateUniqueConstraintSQL(this.metaData, constraint);
			
			this.control.verify();
			
			assert sql.equals("ALTER TABLE 'schema'.'table' ADD UNIQUE uk_name ('column1','column2')") : sql;
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test case for {@link net.sf.hajdbc.Dialect#getDropUniqueConstraintSQL(java.sql.DatabaseMetaData, java.lang.String, java.lang.String, java.lang.String)}
	 */
	@Override
	public void testGetDropUniqueConstraintSQL()
	{
		UniqueConstraint constraint = new UniqueConstraint("uk_name", "schema", "table");
		String quote = "'";
		
		try
		{
			EasyMock.expect(this.metaData.supportsSchemasInDataManipulation()).andReturn(true);
			EasyMock.expect(this.metaData.getIdentifierQuoteString()).andReturn(quote).times(2);
			
			this.control.replay();
			
			String sql = this.dialect.getDropUniqueConstraintSQL(this.metaData, constraint);
			
			this.control.verify();
			
			assert sql.equals("ALTER TABLE 'schema'.'table' DROP INDEX uk_name") : sql;
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}
}
