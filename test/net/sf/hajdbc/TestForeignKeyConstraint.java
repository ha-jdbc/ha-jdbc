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
package net.sf.hajdbc;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Collections;

import net.sf.hajdbc.ForeignKeyConstraint;

import org.easymock.EasyMock;
import org.easymock.IMocksControl;
import org.testng.annotations.Configuration;
import org.testng.annotations.Test;

/**
 * @author  Paul Ferraro
 * @since   1.0
 */
@Test
public class TestForeignKeyConstraint
{
	private IMocksControl control = EasyMock.createControl();
	private Connection connection = this.control.createMock(Connection.class);
	private DatabaseMetaData metaData = this.control.createMock(DatabaseMetaData.class);
	private ResultSet resultSet = this.control.createMock(ResultSet.class);
	
	@Configuration(afterTestMethod = true)
	public void reset()
	{
		this.control.reset();
	}
	
	/**
	 * Test method for {@link ForeignKeyConstraint#collect(Connection, Map)}
	 */
	public void testCollect()
	{
		try
		{
			EasyMock.expect(this.connection.getMetaData()).andReturn(this.metaData);
			
			EasyMock.expect(this.metaData.getImportedKeys(null, "fk_schema", "fk_table")).andReturn(this.resultSet);
			
			EasyMock.expect(this.resultSet.next()).andReturn(true);
			
			EasyMock.expect(this.resultSet.getString("FK_NAME")).andReturn("fk");
			
			EasyMock.expect(this.resultSet.getString("FKCOLUMN_NAME")).andReturn("fk_column1");
			
			EasyMock.expect(this.resultSet.getString("PKTABLE_SCHEM")).andReturn("pk_schema");
			
			EasyMock.expect(this.resultSet.getString("PKTABLE_NAME")).andReturn("pk_table");

			EasyMock.expect(this.resultSet.getString("PKCOLUMN_NAME")).andReturn("pk_column1");

			EasyMock.expect(this.resultSet.next()).andReturn(true);
			
			EasyMock.expect(this.resultSet.getString("FK_NAME")).andReturn("fk");
			
			EasyMock.expect(this.resultSet.getString("FKCOLUMN_NAME")).andReturn("fk_column2");
			
			EasyMock.expect(this.resultSet.getString("PKTABLE_SCHEM")).andReturn("pk_schema");
			
			EasyMock.expect(this.resultSet.getString("PKTABLE_NAME")).andReturn("pk_table");

			EasyMock.expect(this.resultSet.getString("PKCOLUMN_NAME")).andReturn("pk_column2");

			EasyMock.expect(this.resultSet.next()).andReturn(false);
			
			this.resultSet.close();
			
			this.control.replay();
			
			Collection<ForeignKeyConstraint> collection = ForeignKeyConstraint.collect(this.connection, Collections.singletonMap("fk_schema", Collections.singletonList("fk_table")));
			
			this.control.verify();
			
			assert collection != null;
			assert collection.size() == 1 : collection.size();
			
			ForeignKeyConstraint constraint = collection.iterator().next();
			
			assert constraint.getName().equals("fk") : constraint.getName();
			assert constraint.getSchema().equals("fk_schema") : constraint.getSchema();
			assert constraint.getTable().equals("fk_table") : constraint.getTable();
			assert constraint.getColumnList().size() == 2 : constraint.getColumnList().size();
			assert constraint.getColumnList().get(0).equals("fk_column1") : constraint.getColumnList().get(0);
			assert constraint.getColumnList().get(1).equals("fk_column2") : constraint.getColumnList().get(1);
			assert constraint.getForeignSchema().equals("pk_schema") : constraint.getForeignSchema();
			assert constraint.getForeignTable().equals("pk_table") : constraint.getForeignTable();
			assert constraint.getForeignColumnList().size() == 2 : constraint.getForeignColumnList().size();
			assert constraint.getForeignColumnList().get(0).equals("pk_column1") : constraint.getForeignColumnList().get(0);
			assert constraint.getForeignColumnList().get(1).equals("pk_column2") : constraint.getForeignColumnList().get(1);
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}
	
	/**
	 * Test method for {@link ForeignKeyConstraint#collect(Connection, Map)}
	 */
	public void testCollectNoSchema()
	{
		try
		{
			EasyMock.expect(this.connection.getMetaData()).andReturn(this.metaData);
			
			EasyMock.expect(this.metaData.getImportedKeys(null, null, "fk_table")).andReturn(this.resultSet);
			
			EasyMock.expect(this.resultSet.next()).andReturn(true);
			
			EasyMock.expect(this.resultSet.getString("FK_NAME")).andReturn("fk");
			
			EasyMock.expect(this.resultSet.getString("FKCOLUMN_NAME")).andReturn("fk_column1");
			
			EasyMock.expect(this.resultSet.getString("PKTABLE_SCHEM")).andReturn(null);
			
			EasyMock.expect(this.resultSet.getString("PKTABLE_NAME")).andReturn("pk_table");

			EasyMock.expect(this.resultSet.getString("PKCOLUMN_NAME")).andReturn("pk_column1");

			EasyMock.expect(this.resultSet.next()).andReturn(true);
			
			EasyMock.expect(this.resultSet.getString("FK_NAME")).andReturn("fk");
			
			EasyMock.expect(this.resultSet.getString("FKCOLUMN_NAME")).andReturn("fk_column2");
			
			EasyMock.expect(this.resultSet.getString("PKTABLE_SCHEM")).andReturn(null);
			
			EasyMock.expect(this.resultSet.getString("PKTABLE_NAME")).andReturn("pk_table");

			EasyMock.expect(this.resultSet.getString("PKCOLUMN_NAME")).andReturn("pk_column2");

			EasyMock.expect(this.resultSet.next()).andReturn(false);
			
			this.resultSet.close();
			
			this.control.replay();
			
			Collection<ForeignKeyConstraint> collection = ForeignKeyConstraint.collect(this.connection, Collections.singletonMap((String) null, Collections.singletonList("fk_table")));
			
			this.control.verify();
			
			assert collection != null;
			assert collection.size() == 1 : collection.size();
			
			ForeignKeyConstraint constraint = collection.iterator().next();
			
			assert constraint.getName().equals("fk") : constraint.getName();
			assert constraint.getSchema() == null : constraint.getSchema();
			assert constraint.getTable().equals("fk_table") : constraint.getTable();
			assert constraint.getColumnList().size() == 2 : constraint.getColumnList().size();
			assert constraint.getColumnList().get(0).equals("fk_column1") : constraint.getColumnList().get(0);
			assert constraint.getColumnList().get(1).equals("fk_column2") : constraint.getColumnList().get(1);
			assert constraint.getForeignSchema() == null : constraint.getForeignSchema();
			assert constraint.getForeignTable().equals("pk_table") : constraint.getForeignTable();
			assert constraint.getForeignColumnList().size() == 2 : constraint.getForeignColumnList().size();
			assert constraint.getForeignColumnList().get(0).equals("pk_column1") : constraint.getForeignColumnList().get(0);
			assert constraint.getForeignColumnList().get(1).equals("pk_column2") : constraint.getForeignColumnList().get(1);
		}
		catch (SQLException e)
		{
			assert false : e;
		}
	}

	/**
	 * Test method for {@link Constraint#hashCode()}
	 */
	public void testHashCode()
	{
		ForeignKeyConstraint key = new ForeignKeyConstraint("test", null, null);
		
		assert "test".hashCode() == key.hashCode();
	}

	/**
	 * Test method for {@link Constraint#equals(Object)}
	 */
	public void testEqualsObject()
	{
		ForeignKeyConstraint key1 = new ForeignKeyConstraint("test", "", "");
		ForeignKeyConstraint key2 = new ForeignKeyConstraint("test", null, null);
		ForeignKeyConstraint key3 = new ForeignKeyConstraint("testing", null, null);
		
		assert key1.equals(key2);
		assert !key1.equals(key3);
		assert !key2.equals(key3);
	}
}
