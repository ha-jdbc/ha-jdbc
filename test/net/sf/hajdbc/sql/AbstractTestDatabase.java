/*
 * Copyright (c) 2004-2006, Identity Theft 911, LLC.  All rights reserved.
 */
package net.sf.hajdbc.sql;

import net.sf.hajdbc.Database;

import org.easymock.EasyMock;
import org.easymock.IMocksControl;
import org.testng.annotations.Configuration;
import org.testng.annotations.Test;

/**
 * @author Paul Ferraro
 *
 */
public abstract class AbstractTestDatabase
{
	protected IMocksControl control = EasyMock.createControl();
	
	protected abstract Database createDatabase(String id);
	
	@Configuration(afterTestMethod = true)
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
		Database database1 = this.createDatabase("1");
		database1.setWeight(1);
		Database database2 = this.createDatabase("2");
		database2.setWeight(1);
		
		int result = database1.compareTo(database2);
		
		assert result == 0 : result;
		
		database1.setWeight(0);

		result = database1.compareTo(database2);
		
		assert result > 0 : result;
		
		database1.setWeight(2);

		result = database1.compareTo(database2);
		
		assert result < 0 : result;
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
