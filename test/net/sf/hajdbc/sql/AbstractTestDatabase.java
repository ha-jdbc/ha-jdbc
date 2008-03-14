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

import java.util.Properties;

import net.sf.hajdbc.Database;

import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * @author Paul Ferraro
 *
 */
@SuppressWarnings({ "unchecked", "nls" })
public abstract class AbstractTestDatabase<T extends Database, U> implements Database<U>
{
	protected abstract T createDatabase(String id);

	@DataProvider(name = "object")
	protected Object[][] objectProvider()
	{
		return new Object[][] { new Object[] { this.createDatabase("1") }, new Object[] { this.createDatabase("2") }, new Object[] { new Object() }, new Object[] { null } };
	}
	
	/**
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	@Test(dataProvider = "object")
	public boolean equals(Object object)
	{
		Database database = this.createDatabase("1");
		
		boolean equals = database.equals(object);

		boolean expected = (object != null) && Database.class.isInstance(object) && Database.class.cast(object).getId().equals("1");
		
		assert equals == expected : equals;
		
		return equals;
	}
		
	/**
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	@Test
	public int hashCode()
	{
		Database database = this.createDatabase("1");
		
		int hashCode = database.hashCode();
		
		int expected = database.getId().hashCode();
		
		assert hashCode == expected : hashCode;
		
		return hashCode;
	}

	/**
	 * @see java.lang.Object#toString()
	 */
	@Override
	@Test
	public String toString()
	{
		Database database = this.createDatabase("1");
		
		String string = database.toString();
		
		assert string.equals("1") : string;

		return string;
	}

	/**
	 * @see net.sf.hajdbc.Database#clean()
	 */
	@Test
	public void clean()
	{
		Database database = this.createDatabase("1");
		
		database.clean();
		
		assert !database.isDirty();
		
		database.setWeight(1);
		database.clean();
		
		assert !database.isDirty();
	}

	/**
	 * @see net.sf.hajdbc.Database#isDirty()
	 */
	@Test
	public boolean isDirty()
	{
		Database database = this.createDatabase("1");
		
		boolean dirty = database.isDirty();
		
		assert dirty;
		
		database.clean();
		
		dirty = database.isDirty();
		
		assert !dirty;
		
		return dirty;
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
		Database database = this.createDatabase("1");
		
		database.setProperty(name, "value");
		
		String value = database.getProperties().getProperty(name);
		
		assert value.equals("value") : value;
		
		database.clean();
		
		database.removeProperty(name);

		value = database.getProperties().getProperty(name);
		
		assert value == null;
		
		assert database.isDirty();
		
		database.clean();
		database.removeProperty(name);
		
		assert !database.isDirty();
	}

	/**
	 * @see net.sf.hajdbc.InactiveDatabaseMBean#setPassword(java.lang.String)
	 */
	@Test(dataProvider = "string")
	public void setPassword(String password)
	{
		Database database = this.createDatabase("1");
		
		database.setPassword(password);
		
		String value = database.getPassword();
		
		assert value.equals(password) : value;
		
		database.clean();
		
		database.setPassword(password);

		value = database.getPassword();
		
		assert value.equals(password);
		
		assert !database.isDirty();
		
		database.setPassword(null);
		
		assert database.isDirty();
		
		value = database.getPassword();
		
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
		Database database = this.createDatabase("1");
		
		database.clean();
		
		boolean accepted = (name != null) && (value != null);
		
		try
		{
			database.setProperty(name, value);
			
			assert accepted;
			
			String propertyValue = database.getProperties().getProperty(name);
			
			assert propertyValue.equals(value) : propertyValue;
			
			database.clean();
			
			database.setProperty(name, value);
	
			propertyValue = database.getProperties().getProperty(name);
			
			assert propertyValue.equals(value) : propertyValue;
			
			assert !database.isDirty();
			
			database.setProperty(name, "");
			
			assert database.isDirty();
			
			propertyValue = database.getProperties().getProperty(name);
			
			assert propertyValue.equals("") : propertyValue;
		}
		catch (IllegalArgumentException e)
		{
			assert !accepted;
			
			assert !database.isDirty();
		}
	}

	/**
	 * @see net.sf.hajdbc.InactiveDatabaseMBean#setUser(java.lang.String)
	 */
	@Test(dataProvider = "string")
	public void setUser(String user)
	{
		Database database = this.createDatabase("1");
		
		database.setUser(user);
		
		String value = database.getUser();
		
		assert value.equals(user) : value;
		
		database.clean();
		
		database.setUser(user);

		value = database.getUser();
		
		assert value.equals(user);
		
		assert !database.isDirty();
		
		database.setUser(null);
		
		assert database.isDirty();
		
		value = database.getUser();
		
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
		Database database = this.createDatabase("1");
		
		database.setWeight(weight);
		
		int value = database.getWeight();
		
		assert value == weight : value;
		
		database.clean();
		
		database.setWeight(weight);

		value = database.getWeight();
		
		assert value == weight : value;
		
		assert !database.isDirty();
		
		database.setWeight(0);
		
		assert database.isDirty();
		
		value = database.getWeight();
		
		assert value == 0 : value;
		
		database.clean();
		
		try
		{
			database.setWeight(-1);
			
			assert false;
		}
		catch (IllegalArgumentException e)
		{
			assert true;
		}
		
		assert !database.isDirty();
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
		Database database = this.createDatabase("1");
		
		database.setLocal(local);

		boolean value = database.isLocal();
		
		assert value == local : value;
		
		database.clean();

		database.setLocal(local);

		value = database.isLocal();
		
		assert value == local : value;
		
		assert !database.isDirty();
		
		database.setLocal(false);
		
		assert database.isDirty();
		
		value = database.isLocal();
		
		assert !value;
		
		database.clean();
	}
	
	@DataProvider(name = "database")
	protected Object[][] databaseProvider()
	{
		return new Object[][] { new Object[] { this.createDatabase("1") }, new Object[] { this.createDatabase("1") }, new Object[] { this.createDatabase("2") } };
	}

	/**
	 * @see java.lang.Comparable#compareTo(T)
	 */
	@SuppressWarnings("unchecked")
	@Test(dataProvider = "database")
	public int compareTo(Database<U> object)
	{
		Database database = this.createDatabase("1");

		int compared = database.compareTo(object);

		int expected = database.getId().compareTo(object.getId());
		
		assert compared == expected : compared;
		
		return compared;
	}

	/**
	 * @see net.sf.hajdbc.ActiveDatabaseMBean#getId()
	 */
	@Test
	public String getId()
	{
		Database database = this.createDatabase("1");

		String id = database.getId();
		
		assert id.equals("1") : id;
		
		return id;
	}

	/**
	 * @see net.sf.hajdbc.ActiveDatabaseMBean#getPassword()
	 */
	@Test
	public String getPassword()
	{
		Database database = this.createDatabase("1");

		String password = database.getPassword();
		
		assert password == null : password;
		
		database.setPassword("password");
		
		password = database.getPassword();
		
		assert password.equals("password") : password;
		
		return password;
	}

	/**
	 * @see net.sf.hajdbc.ActiveDatabaseMBean#getProperties()
	 */
	@Test
	public Properties getProperties()
	{
		Database database = this.createDatabase("1");

		Properties properties = database.getProperties();
		
		assert properties.isEmpty() : properties;
		
		database.setProperty("name", "value");

		properties = database.getProperties();
		
		assert properties.size() == 1 : properties.size();
		
		String value = properties.getProperty("name");
		
		assert value.equals("value") : value;
		
		database.removeProperty("name");
		
		properties = database.getProperties();
		
		assert properties.isEmpty();
		
		return properties;
	}

	/**
	 * @see net.sf.hajdbc.ActiveDatabaseMBean#getUser()
	 */
	@Test
	public String getUser()
	{
		Database database = this.createDatabase("1");

		String user = database.getUser();
		
		assert user == null : user;
		
		database.setUser("user");
		
		user = database.getUser();
		
		assert user.equals("user") : user;
		
		return user;
	}

	/**
	 * @see net.sf.hajdbc.ActiveDatabaseMBean#getWeight()
	 */
	@Test
	public int getWeight()
	{
		Database database = this.createDatabase("1");

		int weight = database.getWeight();
		
		assert weight == 1 : weight;
		
		database.setWeight(0);
		
		weight = database.getWeight();
		
		assert weight == 0 : weight;
		
		return weight;
	}

	/**
	 * @see net.sf.hajdbc.ActiveDatabaseMBean#isLocal()
	 */
	@Test
	public boolean isLocal()
	{
		Database database = this.createDatabase("1");

		boolean local = database.isLocal();

		assert !local;
		
		database.setLocal(true);
		
		local = database.isLocal();
		
		assert local;
		
		return local;
	}
}
