/*
 * HA-JDBC: High-Availability JDBC
 * Copyright (c) 2004-2007 Paul Ferraro
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

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

import javax.management.DynamicMBean;

import net.sf.hajdbc.Database;

import org.easymock.EasyMock;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * @author Paul Ferraro
 *
 */
@SuppressWarnings({ "unchecked", "nls" })
public abstract class TestDatabase<T extends Database<U>, U> implements Database<U>
{
	protected T database;
	
	@BeforeMethod()
	public void init()
	{
		this.database = this.createDatabase("1");
	}

	protected abstract T createDatabase(String id);
	
	@Test
	public void testCompareTo()
	{
		Database<U> database = EasyMock.createStrictMock(Database.class);
		
		EasyMock.expect(database.getId()).andReturn("0");
		
		EasyMock.replay(database);
		
		int result = this.compareTo(database);
		
		EasyMock.verify(database);
		
		assert result > 0 : result;
		
		EasyMock.reset(database);
		
		EasyMock.expect(database.getId()).andReturn("2");
		
		EasyMock.replay(database);
		
		result = this.compareTo(database);
		
		EasyMock.verify(database);
		
		assert result < 0 : result;
		
		EasyMock.reset(database);
		
		EasyMock.expect(database.getId()).andReturn("1");
		
		EasyMock.replay(database);
		
		result = this.compareTo(database);
		
		EasyMock.verify(database);
		
		assert result == 0 : result;
	}
	
	/**
	 * @see java.lang.Comparable#compareTo(java.lang.Object)
	 */
	@Override
	public int compareTo(Database<U> database)
	{
		return this.database.compareTo(database);
	}

	@Test
	public void testHashCode()
	{
		int hashCode = this.database.hashCode();
		
		int expected = this.database.getId().hashCode();
		
		assert hashCode == expected : hashCode;
	}

	@Test
	public void testToString()
	{
		String string = this.database.toString();
		
		assert string.equals("1") : string;
	}

	/**
	 * @see net.sf.hajdbc.Database#clean()
	 */
	@Test
	public void clean()
	{
		this.database.clean();
		
		assert !this.database.isDirty();
		
		this.database.setWeight(1);
		this.database.clean();
		
		assert !this.database.isDirty();
	}

	@Test
	public void testIsDirty()
	{
		boolean dirty = this.isDirty();
		
		assert dirty;
		
		this.database.clean();
		
		dirty = this.isDirty();
		
		assert !dirty;
	}
	
	/**
	 * @see net.sf.hajdbc.Database#isDirty()
	 */
	@Override
	public boolean isDirty()
	{
		return this.database.isDirty();
	}

	@DataProvider(name = "string")
	protected Object[][] stringProvider()
	{
		return new Object[][] { new Object[] { "test" } };
	}
	
	/**
	 * @see net.sf.hajdbc.InactiveDatabaseMBean#removeProperty(java.lang.String)
	 */
	@Test(dataProvider = "string")
	public void removeProperty(String name)
	{
		this.database.setProperty(name, "value");
		
		String value = this.database.getProperties().getProperty(name);
		
		assert value.equals("value") : value;
		
		this.database.clean();
		
		this.database.removeProperty(name);

		value = this.database.getProperties().getProperty(name);
		
		assert value == null;
		
		assert this.database.isDirty();
		
		this.database.clean();
		this.database.removeProperty(name);
		
		assert !this.database.isDirty();
	}

	/**
	 * @see net.sf.hajdbc.InactiveDatabaseMBean#setPassword(java.lang.String)
	 */
	@Test(dataProvider = "string")
	public void setPassword(String password)
	{
		this.database.setPassword(password);
		
		String value = this.database.getPassword();
		
		assert value.equals(password) : value;
		
		this.database.clean();
		
		this.database.setPassword(password);

		value = this.database.getPassword();
		
		assert value.equals(password);
		
		assert !this.database.isDirty();
		
		this.database.setPassword(null);
		
		assert this.database.isDirty();
		
		value = this.database.getPassword();
		
		assert value == null : value;
	}

	@DataProvider(name = "property")
	protected Object[][] propertyProvider()
	{
		return new Object[][] { new Object[] { "name", "value" }, new Object[] { "name", null }, new Object[] { null, "value" }, new Object[] { null, null } };
	}

	/**
	 * @see net.sf.hajdbc.InactiveDatabaseMBean#setProperty(java.lang.String, java.lang.String)
	 */
	@Test(dataProvider = "property")
	public void setProperty(String name, String value)
	{
		this.database.clean();
		
		boolean accepted = (name != null) && (value != null);
		
		try
		{
			this.database.setProperty(name, value);
			
			assert accepted;
			
			String propertyValue = this.database.getProperties().getProperty(name);
			
			assert propertyValue.equals(value) : propertyValue;
			
			this.database.clean();
			
			this.database.setProperty(name, value);
	
			propertyValue = this.database.getProperties().getProperty(name);
			
			assert propertyValue.equals(value) : propertyValue;
			
			assert !this.database.isDirty();
			
			this.database.setProperty(name, "");
			
			assert this.database.isDirty();
			
			propertyValue = this.database.getProperties().getProperty(name);
			
			assert propertyValue.equals("") : propertyValue;
		}
		catch (IllegalArgumentException e)
		{
			assert !accepted;
			
			assert !this.database.isDirty();
		}
	}

	/**
	 * @see net.sf.hajdbc.InactiveDatabaseMBean#setUser(java.lang.String)
	 */
	@Test(dataProvider = "string")
	public void setUser(String user)
	{
		this.database.setUser(user);
		
		String value = this.database.getUser();
		
		assert value.equals(user) : value;
		
		this.database.clean();
		
		this.database.setUser(user);

		value = this.database.getUser();
		
		assert value.equals(user);
		
		assert !this.database.isDirty();
		
		this.database.setUser(null);
		
		assert this.database.isDirty();
		
		value = this.database.getUser();
		
		assert value == null : value;
	}
	
	@DataProvider(name = "int")
	protected Object[][] intProvider()
	{
		return new Object[][] { new Object[] { 1 } };
	}

	/**
	 * @see net.sf.hajdbc.InactiveDatabaseMBean#setWeight(int)
	 */
	@Test(dataProvider = "int")
	public void setWeight(int weight)
	{
		this.database.setWeight(weight);
		
		int value = this.database.getWeight();
		
		assert value == weight : value;
		
		this.database.clean();
		
		this.database.setWeight(weight);

		value = this.database.getWeight();
		
		assert value == weight : value;
		
		assert !this.database.isDirty();
		
		this.database.setWeight(0);
		
		assert this.database.isDirty();
		
		value = this.database.getWeight();
		
		assert value == 0 : value;
		
		this.database.clean();
		
		try
		{
			this.database.setWeight(-1);
			
			assert false;
		}
		catch (IllegalArgumentException e)
		{
			assert true;
		}
		
		assert !this.database.isDirty();
	}
	
	@DataProvider(name = "boolean")
	protected Object[][] booleanProvider()
	{
		return new Object[][] { new Object[] { true } };
	}

	/**
	 * @see net.sf.hajdbc.InactiveDatabaseMBean#setLocal(boolean)
	 */
	@Test(dataProvider = "boolean")
	public void setLocal(boolean local)
	{
		this.database.setLocal(local);

		boolean value = this.database.isLocal();
		
		assert value == local : value;
		
		this.database.clean();

		this.database.setLocal(local);

		value = this.database.isLocal();
		
		assert value == local : value;
		
		assert !this.database.isDirty();
		
		this.database.setLocal(false);
		
		assert this.database.isDirty();
		
		value = this.database.isLocal();
		
		assert !value;
		
		this.database.clean();
	}

	@Test
	public void testGetId()
	{
		String id = this.getId();
		
		assert id.equals("1") : id;
	}
	
	/**
	 * @see net.sf.hajdbc.ActiveDatabaseMBean#getId()
	 */
	@Override
	public String getId()
	{
		return this.database.getId();
	}

	@Test
	public void testGetPassword()
	{
		String password = this.getPassword();
		
		assert password == null : password;
		
		this.database.setPassword("password");
		
		password = this.getPassword();
		
		assert password.equals("password") : password;
	}
	
	/**
	 * @see net.sf.hajdbc.ActiveDatabaseMBean#getPassword()
	 */
	@Override
	public String getPassword()
	{
		return this.database.getPassword();
	}

	@Test
	public void testGetProperties()
	{
		Properties properties = this.getProperties();
		
		assert properties.isEmpty() : properties;
		
		this.database.setProperty("name", "value");

		properties = this.getProperties();
		
		assert properties.size() == 1 : properties.size();
		
		String value = properties.getProperty("name");
		
		assert value.equals("value") : value;
		
		this.database.removeProperty("name");
		
		properties = this.getProperties();
		
		assert properties.isEmpty();
	}
	
	/**
	 * @see net.sf.hajdbc.ActiveDatabaseMBean#getProperties()
	 */
	@Override
	public Properties getProperties()
	{
		return this.database.getProperties();
	}

	@Test
	public void testGetUser()
	{
		String user = this.getUser();
		
		assert user == null : user;
		
		this.database.setUser("user");
		
		user = this.getUser();
		
		assert user.equals("user") : user;
	}
	
	/**
	 * @see net.sf.hajdbc.ActiveDatabaseMBean#getUser()
	 */
	@Override
	public String getUser()
	{
		return this.database.getUser();
	}

	@Test
	public void testGetWeight()
	{
		int weight = this.getWeight();
		
		assert weight == 1 : weight;
		
		this.database.setWeight(0);
		
		weight = this.getWeight();
		
		assert weight == 0 : weight;
	}
	
	/**
	 * @see net.sf.hajdbc.ActiveDatabaseMBean#getWeight()
	 */
	@Override
	public int getWeight()
	{
		return this.database.getWeight();
	}

	@Test
	public void testIsLocal()
	{
		boolean local = this.isLocal();

		assert !local;
		
		this.database.setLocal(true);
		
		local = this.isLocal();
		
		assert local;
	}
	
	/**
	 * @see net.sf.hajdbc.ActiveDatabaseMBean#isLocal()
	 */
	@Override
	public boolean isLocal()
	{
		return this.database.isLocal();
	}

	public abstract void testConnect() throws SQLException;	

	/**
	 * @see net.sf.hajdbc.Database#connect(java.lang.Object)
	 */
	@Override
	public Connection connect(U connectionFactory) throws SQLException
	{
		return this.database.connect(connectionFactory);
	}
	
	public abstract void testCreateConnectionFactory();	

	/**
	 * @see net.sf.hajdbc.Database#createConnectionSource()
	 */
	@Override
	public U createConnectionSource()
	{
		return this.database.createConnectionSource();
	}

	public abstract void testGetActiveMBean();
	
	/**
	 * @see net.sf.hajdbc.Database#getActiveMBean()
	 */
	@Override
	public DynamicMBean getActiveMBean()
	{
		return this.database.getActiveMBean();
	}

	public abstract void testGetInactiveMBean();
	
	/**
	 * @see net.sf.hajdbc.Database#getInactiveMBean()
	 */
	@Override
	public DynamicMBean getInactiveMBean()
	{
		return this.database.getInactiveMBean();
	}
}
