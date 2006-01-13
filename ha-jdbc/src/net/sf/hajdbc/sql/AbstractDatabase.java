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
public abstract class AbstractDatabase<T> implements Database<T>
{
	protected String id;
	protected String user;
	protected String password;
	protected Properties properties;
	protected Integer weight;

	/**
	 * @see net.sf.hajdbc.Database#getId()
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
		this.id = id;
	}
	
	/**
	 * @return the database user
	 */
	public String getUser()
	{
		return this.user;
	}
	
	/**
	 * @param user
	 */
	public void setUser(String user)
	{
		this.user = user;
	}
	
	/**
	 * @return the password of the database user
	 */
	public String getPassword()
	{
		return this.password;
	}
	
	/**
	 * @param password
	 */
	public void setPassword(String password)
	{
		this.password = password;
	}

	/**
	 * @see net.sf.hajdbc.Database#getWeight()
	 */
	public Integer getWeight()
	{
		return this.weight;
	}
	
	/**
	 * @param weight The weight to set.
	 */
	public void setWeight(Integer weight)
	{
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
	 * Returns a collection of additional properties of this database.
	 * @return additional properties of this database
	 */
	public Properties getProperties()
	{
		return this.properties;
	}
	
	/**
	 * Sets a collection of additional properties for this database.
	 * @param properties
	 */
	public void setProperties(Properties properties)
	{
		this.properties = properties;
	}
}
