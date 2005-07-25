/*
 * HA-JDBC: High-Availability JDBC
 * Copyright (C) 2004 Paul Ferraro
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
import java.sql.Statement;
import java.util.Collection;
import java.util.Collections;

import org.easymock.MockControl;

import net.sf.hajdbc.EasyMockTestCase;

/**
 * @author  Paul Ferraro
 * @since   1.0
 */
public class TestUniqueKey extends EasyMockTestCase
{
	MockControl connectionControl = this.createControl(Connection.class);
	Connection connection = (Connection) this.connectionControl.getMock();
	
	MockControl metaDataControl = this.createControl(DatabaseMetaData.class);
	DatabaseMetaData metaData = (DatabaseMetaData) this.metaDataControl.getMock();
	
	MockControl statementControl = this.createControl(Statement.class);
	Statement statement = (Statement) this.statementControl.getMock();
	
	MockControl resultSetControl = this.createControl(ResultSet.class);
	ResultSet resultSet = (ResultSet) this.resultSetControl.getMock();
	

	/**
	 * Test method for {@link UniqueKey#collect(Connection, String, String)}
	 */
	public void testCollect()
	{
		try
		{
			this.connection.getMetaData();
			this.connectionControl.setReturnValue(this.metaData);
			
			this.metaData.getIdentifierQuoteString();
			this.metaDataControl.setReturnValue("'");

			this.metaData.getIndexInfo(null, null, "table", true, false);
			this.metaDataControl.setReturnValue(this.resultSet);
			
			this.resultSet.next();
			this.resultSetControl.setReturnValue(true);
			
			this.resultSet.getString("INDEX_NAME");
			this.resultSetControl.setReturnValue("pk");
			
			this.resultSet.next();
			this.resultSetControl.setReturnValue(true);

			this.resultSet.getString("INDEX_NAME");
			this.resultSetControl.setReturnValue("idx");
			
			this.resultSet.getShort("ORDINAL_POSITION");
			this.resultSetControl.setReturnValue(1);
			
			this.resultSet.getString("COLUMN_NAME");
			this.resultSetControl.setReturnValue("col1");
			
			this.resultSet.next();
			this.resultSetControl.setReturnValue(true);

			this.resultSet.getString("INDEX_NAME");
			this.resultSetControl.setReturnValue("idx");
			
			this.resultSet.getShort("ORDINAL_POSITION");
			this.resultSetControl.setReturnValue(2);
			
			this.resultSet.getString("COLUMN_NAME");
			this.resultSetControl.setReturnValue("col2");

			this.resultSet.next();
			this.resultSetControl.setReturnValue(false);
			
			this.resultSet.close();
			this.resultSetControl.setVoidCallable();
			
			this.replay();
			
			Collection collection = UniqueKey.collect(this.connection, "table", "pk");
			
			this.verify();
			
			assertNotNull(collection);
			assertEquals(1, collection.size());
			assertEquals(new UniqueKey("idx", "table", "'"), collection.iterator().next());
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}

	/**
	 * Test method for {@link Key#hashCode()}
	 */
	public void testHashCode()
	{
		UniqueKey key = new UniqueKey("test", null, null);
		
		assertEquals("test".hashCode(), key.hashCode());
	}

	/**
	 * Test method for {@link Key#equals(Object)}
	 */
	public void testEqualsObject()
	{
		UniqueKey key1 = new UniqueKey("test", null, null);
		UniqueKey key2 = new UniqueKey("test", "", "");
		UniqueKey key3 = new UniqueKey("testing", "", "");
		
		assertTrue(key1.equals(key2));
		assertFalse(key1.equals(key3));
		assertFalse(key2.equals(key3));
	}

	/**
	 * Test method for {@link Key#executeSQL(Connection, Collection, String)}
	 */
	public void testExecuteSQL()
	{
		try
		{
			this.connection.createStatement();
			this.connectionControl.setReturnValue(this.statement);
			
			this.statement.execute("ALTER TABLE table ADD CONSTRAINT 'idx' UNIQUE (col1, col2)");
			this.statementControl.setReturnValue(true);
			
			this.statement.close();
			this.statementControl.setVoidCallable();
			
			this.replay();
			
			UniqueKey key = new UniqueKey("idx", "table", "'");
			key.addColumn((short) 2, "col2");
			key.addColumn((short) 1, "col1");
			
			UniqueKey.executeSQL(this.connection, Collections.singleton(key), UniqueKey.DEFAULT_CREATE_SQL);
			
			this.verify();
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}
}
