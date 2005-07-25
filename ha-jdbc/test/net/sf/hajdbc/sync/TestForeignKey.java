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
import java.util.List;

import net.sf.hajdbc.EasyMockTestCase;

import org.easymock.MockControl;

/**
 * @author  Paul Ferraro
 * @since   1.0
 */
public class TestForeignKey extends EasyMockTestCase
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
	 * Test method for {@link ForeignKey#collect(Connection, List)}
	 */
	public void testCollect()
	{
		try
		{
			this.connection.getMetaData();
			this.connectionControl.setReturnValue(this.metaData);
			
			this.metaData.getIdentifierQuoteString();
			this.metaDataControl.setReturnValue("'");

			this.metaData.getImportedKeys(null, null, "fk_table");
			this.metaDataControl.setReturnValue(this.resultSet);
			
			this.resultSet.next();
			this.resultSetControl.setReturnValue(true);
			
			this.resultSet.getString("FK_NAME");
			this.resultSetControl.setReturnValue("fk");
			
			this.resultSet.getString("FKCOLUMN_NAME");
			this.resultSetControl.setReturnValue("fk_col");
			
			this.resultSet.getString("PKTABLE_NAME");
			this.resultSetControl.setReturnValue("pk_table");

			this.resultSet.getString("PKCOLUMN_NAME");
			this.resultSetControl.setReturnValue("pk_col");

			this.resultSet.next();
			this.resultSetControl.setReturnValue(false);
			
			this.resultSet.close();
			this.resultSetControl.setVoidCallable();
			
			this.replay();
			
			Collection collection = ForeignKey.collect(this.connection, Collections.singletonList("fk_table"));
			
			this.verify();
			
			assertNotNull(collection);
			assertEquals(1, collection.size());
			assertEquals(new ForeignKey("fk", "fk_table", "fk_col", "pk_table", "pk_col", "'"), collection.iterator().next());
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
		ForeignKey key = new ForeignKey("test", null, null, null, null, null);
		
		assertEquals("test".hashCode(), key.hashCode());
	}

	/**
	 * Test method for {@link Key#equals(Object)}
	 */
	public void testEqualsObject()
	{
		ForeignKey key1 = new ForeignKey("test", "", "", "", "", "");
		ForeignKey key2 = new ForeignKey("test", null, null, null, null, null);
		ForeignKey key3 = new ForeignKey("testing", null, null, null, null, null);
		
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
			
			this.statement.execute("ALTER TABLE fk_table ADD CONSTRAINT 'fk' FOREIGN KEY (fk_col) REFERENCES 'pk_table' (pk_col)");
			this.statementControl.setReturnValue(true);
			
			this.statement.close();
			this.statementControl.setVoidCallable();
			
			this.replay();
			
			ForeignKey key = new ForeignKey("fk", "fk_table", "fk_col", "pk_table", "pk_col", "'");
			
			ForeignKey.executeSQL(this.connection, Collections.singleton(key), ForeignKey.DEFAULT_CREATE_SQL);
			
			this.verify();
		}
		catch (SQLException e)
		{
			this.fail(e);
		}
	}
}
