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
import java.sql.SQLException;

import net.sf.hajdbc.Dialect;
import net.sf.hajdbc.ForeignKeyConstraint;

import org.easymock.EasyMock;
import org.testng.annotations.Test;

/**
 * @author Paul Ferraro
 *
 */
@Test
public class TestMaxDBDialect extends TestDefaultDialect
{
	@Override
	protected Dialect createDialect()
	{
		return new MaxDBDialect();
	}

	@Override
	public void testGetSimpleSQL()
	{
		String sql = this.dialect.getSimpleSQL();
		
		assert sql.equals("SELECT 1 FROM DUAL") : sql;
	}

	@Override
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
			
			assert sql.equals("TRUNCATE TABLE 'schema'.'table'") : sql;
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test case for {@link net.sf.hajdbc.Dialect#getCreateForeignKeyConstraintSQL(java.sql.DatabaseMetaData, java.lang.String, java.lang.String, java.lang.String, java.lang.String, java.lang.String, java.lang.String, java.lang.String)}
	 */
	@Override
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
			
			assert sql.equals("ALTER TABLE 'schema'.'table' ADD CONSTRAINT 'fk_name' FOREIGN KEY ('column1','column2') REFERENCES 'other_schema'.'other_table' ('other_column1','other_column2') ON DELETE NO ACTION") : sql;
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}	
}
