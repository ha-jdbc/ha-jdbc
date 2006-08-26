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

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;

import net.sf.hajdbc.Dialect;
import net.sf.hajdbc.ForeignKeyConstraint;
import net.sf.hajdbc.UniqueConstraint;

import org.easymock.EasyMock;
import org.easymock.IMocksControl;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

/**
 * @author Paul Ferraro
 *
 */
@Test
public class TestDefaultDialect
{
	protected IMocksControl control = EasyMock.createStrictControl();
	protected DatabaseMetaData metaData = this.control.createMock(DatabaseMetaData.class);
	protected ResultSet resultSet = this.control.createMock(ResultSet.class);
	
	protected Dialect dialect = this.createDialect();
	
	protected Dialect createDialect()
	{
		return new DefaultDialect();
	}
	
	@AfterMethod
	public void reset()
	{
		this.control.reset();
	}
	
	/**
	 * Test case for {@link net.sf.hajdbc.Dialect#getSimpleSQL()}
	 */
	public void testGetSimpleSQL()
	{
		String sql = this.dialect.getSimpleSQL();
		
		assert sql.equals("SELECT 1") : sql;
	}

	/**
	 * Test case for {@link net.sf.hajdbc.Dialect#getLockTableSQL(java.sql.DatabaseMetaData, java.lang.String, java.lang.String)}
	 */
	public void testGetLockTableSQL()
	{
		String schema = "schema";
		String table = "table";
		String quote = "'";
		
		try
		{
			EasyMock.expect(this.metaData.supportsSchemasInDataManipulation()).andReturn(true);
			EasyMock.expect(this.metaData.getIdentifierQuoteString()).andReturn(quote).times(2);
			EasyMock.expect(this.metaData.getPrimaryKeys(null, schema, table)).andReturn(this.resultSet);
			EasyMock.expect(this.resultSet.next()).andReturn(true);
			EasyMock.expect(this.resultSet.getString("COLUMN_NAME")).andReturn("column");
			EasyMock.expect(this.resultSet.next()).andReturn(false);
			
			this.resultSet.close();
			
			EasyMock.expect(this.metaData.getIdentifierQuoteString()).andReturn(quote);
			
			this.control.replay();
			
			String sql = this.dialect.getLockTableSQL(this.metaData, schema, table);
			
			this.control.verify();
			
			assert sql.equals("UPDATE 'schema'.'table' SET 'column'='column'") : sql;
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test case for {@link net.sf.hajdbc.Dialect#getTruncateTableSQL(java.sql.DatabaseMetaData, java.lang.String, java.lang.String)}
	 */
	public void testGetTruncateTableSQL()
	{
		String schema = "schema";
		String table = "table";
		String quote = "'";
		
		try
		{
			EasyMock.expect(this.metaData.supportsSchemasInDataManipulation()).andReturn(true);
			EasyMock.expect(this.metaData.getIdentifierQuoteString()).andReturn(quote).times(2);
			
			this.control.replay();
			
			String sql = this.dialect.getTruncateTableSQL(this.metaData, schema, table);
			
			this.control.verify();
			
			assert sql.equals("DELETE FROM 'schema'.'table'") : sql;
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test case for {@link net.sf.hajdbc.Dialect#getCreateForeignKeyConstraintSQL(java.sql.DatabaseMetaData, java.lang.String, java.lang.String, java.lang.String, java.lang.String, java.lang.String, java.lang.String, java.lang.String)}
	 */
	public void testGetCreateForeignKeyConstraintSQL()
	{
		ForeignKeyConstraint constraint = new ForeignKeyConstraint("fk_name", "schema", "table");
		constraint.getColumnList().add("column1");
		constraint.getColumnList().add("column2");
		constraint.setForeignSchema("other_schema");
		constraint.setForeignTable("other_table");
		constraint.getForeignColumnList().add("other_column1");
		constraint.getForeignColumnList().add("other_column2");
		constraint.setUpdateRule(DatabaseMetaData.importedKeyNoAction);
		constraint.setDeleteRule(DatabaseMetaData.importedKeyNoAction);
		constraint.setDeferrability(DatabaseMetaData.importedKeyNotDeferrable);

		String quote = "'";
		
		try
		{
			EasyMock.expect(this.metaData.getIdentifierQuoteString()).andReturn(quote);
			EasyMock.expect(this.metaData.supportsSchemasInDataManipulation()).andReturn(true);
			EasyMock.expect(this.metaData.getIdentifierQuoteString()).andReturn(quote).times(4);
			EasyMock.expect(this.metaData.supportsSchemasInDataManipulation()).andReturn(true);
			EasyMock.expect(this.metaData.getIdentifierQuoteString()).andReturn(quote).times(4);
			
			this.control.replay();
			
			String sql = this.dialect.getCreateForeignKeyConstraintSQL(this.metaData, constraint);
			
			this.control.verify();
			
			assert sql.equals("ALTER TABLE 'schema'.'table' ADD CONSTRAINT 'fk_name' FOREIGN KEY ('column1','column2') REFERENCES 'other_schema'.'other_table' ('other_column1','other_column2') ON DELETE NO ACTION ON UPDATE NO ACTION NOT DEFERRABLE") : sql;
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test case for {@link net.sf.hajdbc.Dialect#getDropForeignKeyConstraintSQL(java.sql.DatabaseMetaData, java.lang.String, java.lang.String, java.lang.String)}
	 */
	public void testGetDropForeignKeyConstraintSQL()
	{
		ForeignKeyConstraint constraint = new ForeignKeyConstraint("fk_name", "schema", "table");
		String quote = "'";
		
		try
		{
			EasyMock.expect(this.metaData.getIdentifierQuoteString()).andReturn(quote);
			EasyMock.expect(this.metaData.supportsSchemasInDataManipulation()).andReturn(true);
			EasyMock.expect(this.metaData.getIdentifierQuoteString()).andReturn(quote).times(2);
			
			this.control.replay();
			
			String sql = this.dialect.getDropForeignKeyConstraintSQL(this.metaData, constraint);
			
			this.control.verify();
			
			assert sql.equals("ALTER TABLE 'schema'.'table' DROP CONSTRAINT 'fk_name'") : sql;
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test case for {@link net.sf.hajdbc.Dialect#getCreateUniqueConstraintSQL(java.sql.DatabaseMetaData, java.lang.String, java.lang.String, java.lang.String, java.util.List)}
	 */
	public void testGetCreateUniqueConstraintSQL()
	{
		UniqueConstraint constraint = new UniqueConstraint("uk_name", "schema", "table");
		constraint.getColumnList().add("column1");
		constraint.getColumnList().add("column2");
		String quote = "'";
		
		try
		{
			EasyMock.expect(this.metaData.getIdentifierQuoteString()).andReturn(quote);
			EasyMock.expect(this.metaData.supportsSchemasInDataManipulation()).andReturn(true);
			EasyMock.expect(this.metaData.getIdentifierQuoteString()).andReturn(quote).times(4);
			
			this.control.replay();
			
			String sql = this.dialect.getCreateUniqueConstraintSQL(this.metaData, constraint);
			
			this.control.verify();
			
			assert sql.equals("ALTER TABLE 'schema'.'table' ADD CONSTRAINT 'uk_name' UNIQUE ('column1','column2')") : sql;
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test case for {@link net.sf.hajdbc.Dialect#getDropUniqueConstraintSQL(java.sql.DatabaseMetaData, java.lang.String, java.lang.String, java.lang.String)}
	 */
	public void testGetDropUniqueConstraintSQL()
	{
		UniqueConstraint constraint = new UniqueConstraint("uk_name", "schema", "table");
		String quote = "'";
		
		try
		{
			EasyMock.expect(this.metaData.getIdentifierQuoteString()).andReturn(quote);
			EasyMock.expect(this.metaData.supportsSchemasInDataManipulation()).andReturn(true);
			EasyMock.expect(this.metaData.getIdentifierQuoteString()).andReturn(quote).times(2);
			
			this.control.replay();
			
			String sql = this.dialect.getDropUniqueConstraintSQL(this.metaData, constraint);
			
			this.control.verify();
			
			assert sql.equals("ALTER TABLE 'schema'.'table' DROP CONSTRAINT 'uk_name'") : sql;
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test case for {@link net.sf.hajdbc.Dialect#qualifyTable(java.sql.DatabaseMetaData, java.lang.String, java.lang.String)}
	 */
	public void testQualifyTable()
	{
		String schema = "schema";
		String table = "table";
		String quote = "'";
		
		try
		{
			EasyMock.expect(this.metaData.supportsSchemasInDataManipulation()).andReturn(true);
			EasyMock.expect(this.metaData.getIdentifierQuoteString()).andReturn(quote).times(2);
			
			this.control.replay();
			
			String sql = this.dialect.qualifyTable(this.metaData, schema, table);
			
			this.control.verify();
			
			assert sql.equals("'schema'.'table'") : sql;
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test case for {@link net.sf.hajdbc.Dialect#qualifyTable(java.sql.DatabaseMetaData, java.lang.String, java.lang.String)}
	 */
	public void testQualifyTableNoSchema()
	{
		String schema = null;
		String table = "table";
		String quote = "'";
		
		try
		{
			EasyMock.expect(this.metaData.getIdentifierQuoteString()).andReturn(quote);
			
			this.control.replay();
			
			String sql = this.dialect.qualifyTable(this.metaData, schema, table);
			
			this.control.verify();
			
			assert sql.equals("'table'") : sql;
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test case for {@link net.sf.hajdbc.Dialect#qualifyTable(java.sql.DatabaseMetaData, java.lang.String, java.lang.String)}
	 */
	public void testQualifyTableSchemaNotSupported()
	{
		String schema = "schema";
		String table = "table";
		String quote = "'";
		
		try
		{
			EasyMock.expect(this.metaData.supportsSchemasInDataManipulation()).andReturn(false);
			EasyMock.expect(this.metaData.getIdentifierQuoteString()).andReturn(quote);
			
			this.control.replay();
			
			String sql = this.dialect.qualifyTable(this.metaData, schema, table);
			
			this.control.verify();
			
			assert sql.equals("'table'") : sql;
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test case for {@link net.sf.hajdbc.Dialect#quote(java.sql.DatabaseMetaData, java.lang.String)}
	 */
	public void testQuote()
	{
		String identifier = "blah";
		String quote = "'";
		
		try
		{
			EasyMock.expect(this.metaData.getIdentifierQuoteString()).andReturn(quote);
			
			this.control.replay();
			
			String sql = this.dialect.quote(this.metaData, identifier);
			
			this.control.verify();
			
			assert sql.equals("'blah'") : sql;
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test case for {@link net.sf.hajdbc.Dialect#isSelectForUpdate(java.sql.DatabaseMetaData, java.lang.String)}
	 */
	public void testIsSelectForUpdate()
	{
		String sql = "SELECT * FROM table FOR UPDATE";
		
		try
		{
			EasyMock.expect(this.metaData.supportsSelectForUpdate()).andReturn(true);
			
			this.control.replay();
			
			boolean result = this.dialect.isSelectForUpdate(this.metaData, sql);
			
			this.control.verify();
			
			assert result;
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test case for {@link net.sf.hajdbc.Dialect#isSelectForUpdate(java.sql.DatabaseMetaData, java.lang.String)}
	 */
	public void testIsSelectForUpdateFalse()
	{
		String sql = "SELECT * FROM table";
		
		try
		{
			EasyMock.expect(this.metaData.supportsSelectForUpdate()).andReturn(true);
			
			this.control.replay();
			
			boolean result = this.dialect.isSelectForUpdate(this.metaData, sql);
			
			this.control.verify();
			
			assert !result;
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test case for {@link net.sf.hajdbc.Dialect#isSelectForUpdate(java.sql.DatabaseMetaData, java.lang.String)}
	 */
	public void testIsSelectForUpdateNotSupported()
	{
		String sql = "SELECT blah FOR UPDATE";
		
		try
		{
			EasyMock.expect(this.metaData.supportsSelectForUpdate()).andReturn(false);
			
			this.control.replay();
			
			boolean result = this.dialect.isSelectForUpdate(this.metaData, sql);
			
			this.control.verify();
			
			assert !result;
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}
}
