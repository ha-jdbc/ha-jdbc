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
package net.sf.hajdbc.sync;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Collections;

import net.sf.hajdbc.EasyMockTestCase;

import org.easymock.EasyMock;

/**
 * @author  Paul Ferraro
 * @since   1.0
 */
public class TestForeignKeyConstraint extends EasyMockTestCase
{
	private Connection connection = this.control.createMock(Connection.class);
	private DatabaseMetaData metaData = this.control.createMock(DatabaseMetaData.class);
	private ResultSet resultSet = this.control.createMock(ResultSet.class);
	
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
			
			EasyMock.expect(this.resultSet.getString("FKCOLUMN_NAME")).andReturn("fk_column");
			
			EasyMock.expect(this.resultSet.getString("PKTABLE_SCHEM")).andReturn("pk_schema");
			
			EasyMock.expect(this.resultSet.getString("PKTABLE_NAME")).andReturn("pk_table");

			EasyMock.expect(this.resultSet.getString("PKCOLUMN_NAME")).andReturn("pk_column");

			EasyMock.expect(this.resultSet.next()).andReturn(false);
			
			this.resultSet.close();
			
			this.control.replay();
			
			Collection<ForeignKeyConstraint> collection = ForeignKeyConstraint.collect(this.connection, Collections.singletonMap("fk_schema", Collections.singletonList("fk_table")));
			
			this.control.verify();
			
			assertNotNull(collection);
			assertEquals(1, collection.size());
			
			ForeignKeyConstraint constraint = collection.iterator().next();
			
			assertEquals("fk", constraint.getName());
			assertEquals("fk_schema", constraint.getSchema());
			assertEquals("fk_table", constraint.getTable());
			assertEquals("fk_column", constraint.getColumn());
			assertEquals("pk_schema", constraint.getForeignSchema());
			assertEquals("pk_table", constraint.getForeignTable());
			assertEquals("pk_column", constraint.getForeignColumn());
		}
		catch (SQLException e)
		{
			fail(e);
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
			
			EasyMock.expect(this.resultSet.getString("FKCOLUMN_NAME")).andReturn("fk_column");
			
			EasyMock.expect(this.resultSet.getString("PKTABLE_SCHEM")).andReturn(null);
			
			EasyMock.expect(this.resultSet.getString("PKTABLE_NAME")).andReturn("pk_table");

			EasyMock.expect(this.resultSet.getString("PKCOLUMN_NAME")).andReturn("pk_column");

			EasyMock.expect(this.resultSet.next()).andReturn(false);
			
			this.resultSet.close();
			
			this.control.replay();
			
			Collection<ForeignKeyConstraint> collection = ForeignKeyConstraint.collect(this.connection, Collections.singletonMap((String) null, Collections.singletonList("fk_table")));
			
			this.control.verify();
			
			assertNotNull(collection);
			assertEquals(1, collection.size());
			
			ForeignKeyConstraint constraint = collection.iterator().next();
			
			assertEquals("fk", constraint.getName());
			assertNull(constraint.getSchema());
			assertEquals("fk_table", constraint.getTable());
			assertEquals("fk_column", constraint.getColumn());
			assertNull(constraint.getForeignSchema());
			assertEquals("pk_table", constraint.getForeignTable());
			assertEquals("pk_column", constraint.getForeignColumn());
		}
		catch (SQLException e)
		{
			fail(e);
		}
	}

	/**
	 * Test method for {@link Constraint#hashCode()}
	 */
	public void testHashCode()
	{
		ForeignKeyConstraint key = new ForeignKeyConstraint("test", null, null, null, null, null, null);
		
		assertEquals("test".hashCode(), key.hashCode());
	}

	/**
	 * Test method for {@link Constraint#equals(Object)}
	 */
	public void testEqualsObject()
	{
		ForeignKeyConstraint key1 = new ForeignKeyConstraint("test", "", "", "", "", "", "");
		ForeignKeyConstraint key2 = new ForeignKeyConstraint("test", null, null, null, null, null, null);
		ForeignKeyConstraint key3 = new ForeignKeyConstraint("testing", null, null, null, null, null, null);
		
		assertTrue(key1.equals(key2));
		assertFalse(key1.equals(key3));
		assertFalse(key2.equals(key3));
	}
}
