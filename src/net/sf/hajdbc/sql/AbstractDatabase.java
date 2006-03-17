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

/**
 * @author  Paul Ferraro
 * @version $Revision$
 * @param <T> 
 * @since   1.0
 */
/**
 * @author Paul Ferraro
 *
 * @param <T>
 */
/**
 * @author Paul Ferraro
 *
 * @param <T>
 */
public abstract class AbstractDatabase<T> implements Database<T>
{
	protected String id;
	protected String user;
	protected String password;
	protected Properties properties = new Properties();
	protected int weight = 1;
	protected boolean dirty = false;

	/**
	 * @see net.sf.hajdbc.ActiveDatabaseMBean#getId()
	 */
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
	public String getUser()
	{
		return this.user;
	}
	
	/**
	 * @see net.sf.hajdbc.InactiveDatabaseMBean#setUser(java.lang.String)
	 */
	public void setUser(String user)
	{
		this.checkDirty(this.user, user);
		this.user = user;
	}
	
	/**
	 * @see net.sf.hajdbc.ActiveDatabaseMBean#getPassword()
	 */
	public String getPassword()
	{
		return this.password;
	}
	
	/**
	 * @see net.sf.hajdbc.InactiveDatabaseMBean#setPassword(java.lang.String)
	 */
	public void setPassword(String password)
	{
		this.checkDirty(this.password, password);
		this.password = password;
	}

	/**
	 * @see net.sf.hajdbc.ActiveDatabaseMBean#getWeight()
	 */
	public int getWeight()
	{
		return this.weight;
	}
	
	/**
	 * @see net.sf.hajdbc.InactiveDatabaseMBean#setWeight(int)
	 */
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
	public int hashCode()
	{
		return this.id.hashCode();
	}
	
	/**
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	public boolean equals(Object object)
	{
		if ((object == null) || !Database.class.isInstance(object))
		{
			return false;
		}
		
		Database database = (Database) object;
		
		return this.id.equals(database.getId());
	}
	
	/**
	 * @see java.lang.Object#toString()
	 */
	public String toString()
	{
		return this.id;
	}
	
	/**
	 * @see net.sf.hajdbc.ActiveDatabaseMBean#getProperties()
	 */
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
	public void removeProperty(String name)
	{
		this.dirty |= this.properties.containsKey(name);
		this.properties.remove(name);
	}

	/**
	 * @see net.sf.hajdbc.InactiveDatabaseMBean#setProperty(java.lang.String, java.lang.String)
	 */
	public void setProperty(String name, String value)
	{
		this.checkDirty(this.properties.getProperty(name), value);
		this.properties.setProperty(name, value);
	}

	/**
	 * @see net.sf.hajdbc.Database#clean()
	 */
	public void clean()
	{
		this.dirty = false;
	}

	/**
	 * @see net.sf.hajdbc.Database#isDirty()
	 */
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
	 * @see java.lang.Comparable#compareTo(T)
	 */
	public int compareTo(Database database)
	{
		return Integer.valueOf(database.getWeight()).compareTo(this.weight);
	}
}
