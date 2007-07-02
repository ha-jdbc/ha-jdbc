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

/**
 * @author  Paul Ferraro
 * @param <D> either java.sql.Driver or javax.sql.DataSource
 * @since   1.0
 */
public abstract class AbstractDatabase<D> implements Database<D>
{
	private String id;
	protected String user;
	protected String password;
	protected Properties properties = new Properties();
	private int weight = 1;
	private boolean dirty = false;

	/**
	 * @see net.sf.hajdbc.ActiveDatabaseMBean#getId()
	 */
	@Override
	public String getId()
	{
		return this.id;
	}
	
	/**
	 * @param id
	 */
	public void setId(String id)
	{
		this.checkDirty(this.id, id);
		this.id = id;
	}
	
	/**
	 * @see net.sf.hajdbc.ActiveDatabaseMBean#getUser()
	 */
	@Override
	public String getUser()
	{
		return this.user;
	}
	
	/**
	 * @see net.sf.hajdbc.InactiveDatabaseMBean#setUser(java.lang.String)
	 */
	@Override
	public void setUser(String user)
	{
		this.checkDirty(this.user, user);
		this.user = user;
	}
	
	/**
	 * @see net.sf.hajdbc.ActiveDatabaseMBean#getPassword()
	 */
	@Override
	public String getPassword()
	{
		return this.password;
	}
	
	/**
	 * @see net.sf.hajdbc.InactiveDatabaseMBean#setPassword(java.lang.String)
	 */
	@Override
	public void setPassword(String password)
	{
		this.checkDirty(this.password, password);
		this.password = password;
	}

	/**
	 * @see net.sf.hajdbc.ActiveDatabaseMBean#getWeight()
	 */
	@Override
	public int getWeight()
	{
		return this.weight;
	}
	
	/**
	 * @see net.sf.hajdbc.InactiveDatabaseMBean#setWeight(int)
	 */
	@Override
	public void setWeight(int weight)
	{
		if (weight < 0)
		{
			throw new IllegalArgumentException();
		}
		
		this.checkDirty(this.weight, weight);
		this.weight = weight;
	}
	
	/**
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode()
	{
		return this.id.hashCode();
	}
	
	/**
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@SuppressWarnings("unchecked")
	@Override
	public boolean equals(Object object)
	{
		if ((object == null) || !(object instanceof Database)) return false;
		
		String id = ((Database) object).getId();
		
		return (id != null) && id.equals(this.id);
	}
	
	/**
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString()
	{
		return this.id;
	}
	
	/**
	 * @see net.sf.hajdbc.ActiveDatabaseMBean#getProperties()
	 */
	@Override
	public Properties getProperties()
	{
		return this.properties;
	}
	
	/**
	 * @param properties
	 */
	public void setProperties(Properties properties)
	{
		this.checkDirty(this.properties, properties);
		this.properties = properties;
	}

	/**
	 * @see net.sf.hajdbc.InactiveDatabaseMBean#removeProperty(java.lang.String)
	 */
	@Override
	public void removeProperty(String name)
	{
		this.dirty |= this.properties.containsKey(name);
		this.properties.remove(name);
	}

	/**
	 * @see net.sf.hajdbc.InactiveDatabaseMBean#setProperty(java.lang.String, java.lang.String)
	 */
	@Override
	public void setProperty(String name, String value)
	{
		if ((name == null) || (value == null))
		{
			throw new IllegalArgumentException();
		}
		
		this.checkDirty(this.properties.getProperty(name), value);
		this.properties.setProperty(name, value);
	}

	/**
	 * @see net.sf.hajdbc.Database#clean()
	 */
	@Override
	public void clean()
	{
		this.dirty = false;
	}

	/**
	 * @see net.sf.hajdbc.Database#isDirty()
	 */
	@Override
	public boolean isDirty()
	{
		return this.dirty;
	}
	
	/**
	 * Set the dirty flag if the new value differs from the old value.
	 * @param oldValue
	 * @param newValue
	 */
	protected void checkDirty(Object oldValue, Object newValue)
	{
		this.dirty |= ((oldValue != null) && (newValue != null)) ? !oldValue.equals(newValue) : (oldValue != newValue);
	}

	/**
	 * @see java.lang.Comparable#compareTo(Object)
	 */
	@Override
	public int compareTo(Database<D> database)
	{
		return this.id.compareTo(database.getId());
	}
}
