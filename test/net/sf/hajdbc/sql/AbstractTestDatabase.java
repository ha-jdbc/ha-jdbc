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
package net.sf.hajdbc.sql;

import net.sf.hajdbc.Database;

import org.easymock.EasyMock;
import org.easymock.IMocksControl;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

/**
 * @author Paul Ferraro
 *
 */
public abstract class AbstractTestDatabase
{
	protected IMocksControl control = EasyMock.createControl();
	
	protected abstract Database createDatabase(String id);
	
	@AfterMethod
	protected void tearDown()
	{
		this.control.reset();
	}
	
	/**
	 * Test method for {@link net.sf.hajdbc.sql.AbstractDatabase#equals(Object)}
	 */
	@Test
	public void testEqualsObject()
	{
		Database database1 = this.createDatabase("1");
		
		Database database2 = this.createDatabase("1");
		
		assert database1.equals(database2);
		
		database2 = this.createDatabase("2");

		assert !database1.equals(database2);
	}

	/**
	 * Test method for {@link net.sf.hajdbc.sql.AbstractDatabase#hashCode()}
	 */
	@Test
	public void testHashCode()
	{
		Database database = this.createDatabase("test");
		
		int hashCode = database.hashCode();
		
		assert "test".hashCode() == hashCode;
	}
	
	/**
	 * Test method for {@link net.sf.hajdbc.sql.AbstractDatabase#compareTo(Database)}
	 */
	@Test
	public void testCompareTo()
	{
		Database database1 = this.createDatabase("a");
		Database database2 = this.createDatabase("b");
		
		int result = database1.compareTo(database2);
		
		assert result < 0 : result;
		
		result = database2.compareTo(database1);
		
		assert result > 0 : result;
		
		database2 = this.createDatabase("a");
		
		result = database1.compareTo(database2);
		
		assert result == 0 : result;
		
		result = database2.compareTo(database1);
		
		assert result == 0 : result;
	}
	
	/**
	 * Test method for {@link net.sf.hajdbc.sql.AbstractDatabase#setUser(String)}
	 */
	@Test
	public void testSetUser()
	{
		Database database = this.createDatabase(null);
		
		assert !database.isDirty();
		
		database.setUser(null);
		
		assert !database.isDirty();
		
		database.setUser("test");
		
		assert database.isDirty();

		database.setUser("test");
		
		assert database.isDirty();

		database.clean();
		
		assert !database.isDirty();
		
		database.setUser(null);
		
		assert database.isDirty();
		
		database.setUser("test");
		
		assert database.isDirty();
		
		database.clean();
		
		assert !database.isDirty();
		
		database.setUser("different");
		
		assert database.isDirty();
	}
	
	/**
	 * Test method for {@link net.sf.hajdbc.sql.AbstractDatabase#setPassword(String)}
	 */
	@Test
	public void testSetPassword()
	{
		Database database = this.createDatabase(null);
		
		assert !database.isDirty();
		
		database.setPassword(null);
		
		assert !database.isDirty();
		
		database.setPassword("test");
		
		assert database.isDirty();

		database.setPassword("test");
		
		assert database.isDirty();

		database.clean();
		
		assert !database.isDirty();
		
		database.setPassword(null);
		
		assert database.isDirty();
		
		database.setPassword("test");
		
		assert database.isDirty();
		
		database.clean();
		
		assert !database.isDirty();
		
		database.setUser("different");
		
		assert database.isDirty();
	}
	
	/**
	 * Test method for {@link net.sf.hajdbc.sql.AbstractDatabase#setProperty(String, String)}
	 */
	@Test
	public void testSetProperty()
	{
		Database database = this.createDatabase(null);
		
		assert !database.isDirty();
		
		database.setProperty("name", "test");
		
		assert database.isDirty();

		database.setProperty("name", "test");
		
		assert database.isDirty();

		database.clean();
		
		assert !database.isDirty();
		
		database.setProperty("name", "different");
		
		assert database.isDirty();
	}
	
	/**
	 * Test method for {@link net.sf.hajdbc.sql.AbstractDatabase#setWeight(int)}
	 */
	@Test
	public void testSetWeight()
	{
		Database database = this.createDatabase(null);
		
		assert !database.isDirty();
		
		database.setWeight(1);
		
		assert !database.isDirty();
		
		database.setWeight(2);
		
		assert database.isDirty();

		database.setWeight(2);
		
		assert database.isDirty();
	}
	
	/**
	 * Test method for {@link net.sf.hajdbc.sql.AbstractDatabase#removeProperty(String)}
	 */
	@Test
	public void testRemoveProperty()
	{
		Database database = this.createDatabase(null);
		
		assert !database.isDirty();
		
		database.removeProperty("name");
		
		assert !database.isDirty();
		
		database.setProperty("name", "test");
		
		assert database.isDirty();

		database.clean();
		
		assert !database.isDirty();

		database.removeProperty("name");
		
		assert database.isDirty();
	}
}
