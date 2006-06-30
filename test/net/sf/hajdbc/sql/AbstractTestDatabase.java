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

import java.util.Properties;

import net.sf.hajdbc.Database;

import org.easymock.EasyMock;
import org.easymock.IMocksControl;
import org.testng.annotations.Configuration;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * @author Paul Ferraro
 *
 */
public abstract class AbstractTestDatabase<T extends Database, U> implements Database<U>
{
	protected IMocksControl control = EasyMock.createControl();
	protected T database;
	
	protected abstract T createDatabase(String id);
	
	@Configuration(beforeTestMethod = true)
	protected void setup()
	{
		this.database = this.createDatabase("1");
	}
	
	@Configuration(afterTestMethod = true)
	protected void tearDown()
	{
		this.control.reset();
	}
	
	@DataProvider(name = "database")
	protected Object[][] databaseParameters()
	{
		return new Object[][] { new Object[] { this.createDatabase("1") } };
	}
	
	/**
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	@Test(dataProvider = "database")
	public boolean equals(Object object)
	{
		boolean equals = this.database.equals(object);
		
		assert equals;
		
		equals = this.database.equals(this.createDatabase("2"));
		
		assert !equals;
		
		return equals;
	}

	@DataProvider(name = "string")
	public Object[][] stringParameters()
	{
		return new Object[][] { new Object[] { "test" } };
	}
	
	@DataProvider(name = "property")
	public Object[][] propertyParameters()
	{
		return new Object[][] { new Object[] { "name", "value" } };
	}
		
	@DataProvider(name = "weight")
	public Object[][] weightParameters()
	{
		return new Object[][] { new Object[] { 0 } };
	}
		
	/**
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	@Test
	public int hashCode()
	{
		int hashCode = this.database.hashCode();
		
		assert hashCode == this.database.getId().hashCode() : hashCode;
		
		return hashCode;
	}

	/**
	 * @see java.lang.Object#toString()
	 */
	@Override
	@Test
	public String toString()
	{
		String string = this.database.toString();
		
		assert string.equals(this.database.getId()) : string;

		return string;
	}

	/**
	 * @see net.sf.hajdbc.Database#clean()
	 */
	@Test
	public void clean()
	{
		assert this.database.isDirty();
		
		this.database.clean();
		
		assert !this.database.isDirty();
	}

	/**
	 * @see net.sf.hajdbc.Database#isDirty()
	 */
	@Test
	public boolean isDirty()
	{
		boolean dirty = this.database.isDirty();
		
		assert dirty;
		
		return dirty;
	}

	/**
	 * @see net.sf.hajdbc.InactiveDatabaseMBean#removeProperty(java.lang.String)
	 */
	@Test(dataProvider = "string")
	public void removeProperty(String name)
	{
		this.database.setProperty(name, "value");
		
		this.database.clean();
		
		assert !this.database.isDirty();
		
		this.database.removeProperty(name);
		
		assert this.database.isDirty();
		
		this.database.clean();
		
		assert !this.database.isDirty();
		
		this.database.removeProperty(name);
		
		assert !this.database.isDirty();
	}

	/**
	 * @see net.sf.hajdbc.InactiveDatabaseMBean#setPassword(java.lang.String)
	 */
	@Test(dataProvider = "string")
	public void setPassword(String password)
	{
		this.database.clean();
		
		this.database.setPassword(password);
		
		String pass = this.database.getPassword();
		
		assert pass.equals(password) : pass;
		
		assert this.database.isDirty();
		
		this.database.clean();
		
		this.database.setPassword(password);
		
		assert !this.database.isDirty();
	}

	/**
	 * @see net.sf.hajdbc.InactiveDatabaseMBean#setProperty(java.lang.String, java.lang.String)
	 */
	@Test(dataProvider = "property")
	public void setProperty(String name, String value)
	{
		this.database.clean();
		
		this.database.setProperty(name, value);
		
		String propertyValue = this.database.getProperties().getProperty(name);
		
		assert propertyValue.equals(value) : propertyValue;
		
		assert this.database.isDirty();
		
		this.database.clean();
		
		this.database.setProperty(name, value);
		
		assert !this.database.isDirty();
	}

	/**
	 * @see net.sf.hajdbc.InactiveDatabaseMBean#setUser(java.lang.String)
	 */
	@Test(dataProvider = "string")
	public void setUser(String user)
	{
		this.database.clean();
		
		this.database.setUser(user);
		
		String username = this.database.getUser();
		
		assert username.equals(user) : username;
		
		assert this.database.isDirty();
		
		this.database.clean();
		
		this.database.setUser(user);
		
		assert !this.database.isDirty();
	}

	/**
	 * @see net.sf.hajdbc.InactiveDatabaseMBean#setWeight(int)
	 */
	@Test(dataProvider = "weight")
	public void setWeight(int weight)
	{
		this.database.clean();
		
		this.database.setWeight(weight);
		
		int w = this.database.getWeight();
		
		assert w == weight : w;
		
		assert this.database.isDirty();
		
		this.database.clean();
		
		this.database.setWeight(weight);
		
		assert !this.database.isDirty();
	}

	/**
	 * @see java.lang.Comparable#compareTo(T)
	 */
	@SuppressWarnings("unchecked")
	@Test(dataProvider = "database")
	public int compareTo(Database object)
	{
		int compare = this.database.compareTo(object);
		
		assert compare == 0 : compare;
		
		T database = this.createDatabase("2");
		
		compare = this.database.compareTo(database);
		
		assert compare == this.database.getId().compareTo(database.getId()) : compare;
		
		return compare;
	}

	/**
	 * @see net.sf.hajdbc.ActiveDatabaseMBean#getId()
	 */
	public String getId()
	{
		String id = this.database.getId();
		
		assert id.equals("1") : id;
		
		return id;
	}

	/**
	 * @see net.sf.hajdbc.ActiveDatabaseMBean#getPassword()
	 */
	public String getPassword()
	{
		String password = this.database.getPassword();
		
		assert password == null : password;
		
		return password;
	}

	/**
	 * @see net.sf.hajdbc.ActiveDatabaseMBean#getProperties()
	 */
	public Properties getProperties()
	{
		Properties properties = this.database.getProperties();
		
		assert properties.isEmpty();
		
		return properties;
	}

	/**
	 * @see net.sf.hajdbc.ActiveDatabaseMBean#getUser()
	 */
	public String getUser()
	{
		String user = this.database.getUser();
		
		assert user == null : user;
		
		return user;
	}

	/**
	 * @see net.sf.hajdbc.ActiveDatabaseMBean#getWeight()
	 */
	public int getWeight()
	{
		int weight = this.database.getWeight();
		
		assert weight == 1 : weight;
		
		return weight;
	}
}
