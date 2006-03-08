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

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Properties;
import java.util.Set;

import net.sf.hajdbc.Database;
import net.sf.hajdbc.util.Collections;

/**
 * @author  Paul Ferraro
 * @version $Revision$
 * @param <T> 
 * @since   1.0
 */
public abstract class AbstractDatabase<T> implements Database<T>, Externalizable
{
	protected String id;
	protected String user;
	protected String password;
	protected Properties properties = new Properties();
	protected int weight = 1;

	/**
	 * @see net.sf.hajdbc.DatabaseMBean#getId()
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
	 * @see net.sf.hajdbc.DatabaseMBean#getUser()
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
	 * @see net.sf.hajdbc.DatabaseMBean#getPassword()
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
	 * @see net.sf.hajdbc.DatabaseMBean#getWeight()
	 */
	public int getWeight()
	{
		return this.weight;
	}
	
	/**
	 * @param weight The weight to set.
	 */
	public void setWeight(int weight)
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
	 * @see net.sf.hajdbc.DatabaseMBean#getProperties()
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

	/**
	 * @see java.io.Externalizable#readExternal(java.io.ObjectInput)
	 */
	public void readExternal(ObjectInput input) throws IOException, ClassNotFoundException
	{
		this.id = input.readUTF();
		this.weight = input.readInt();
		this.user = input.readUTF();
		this.password = input.readUTF();
		
		int count = input.readInt();
		
		this.properties = new Properties();
		
		for (int i = 0; i < count; ++i)
		{
			this.properties.setProperty(input.readUTF(), input.readUTF());
		}
	}

	/**
	 * @see java.io.Externalizable#writeExternal(java.io.ObjectOutput)
	 */
	public void writeExternal(ObjectOutput output) throws IOException
	{
		output.writeUTF(this.id);
		output.writeInt(this.weight);
		output.writeUTF(this.user);
		output.writeUTF(this.password);
		
		output.writeInt(this.properties.size());
		
		Set<String> propertySet = Collections.cast(this.properties.keySet(), String.class);
		
		for (String property: propertySet)
		{
			output.writeUTF(property);
			output.writeUTF(this.properties.getProperty(property));
		}
	}

	/**
	 * @see net.sf.hajdbc.DatabaseMBean#removeProperty(java.lang.String)
	 */
	public void removeProperty(String name)
	{
		this.properties.remove(name);
	}

	/**
	 * @see net.sf.hajdbc.DatabaseMBean#setProperty(java.lang.String, java.lang.String)
	 */
	public void setProperty(String name, String value)
	{
		this.properties.setProperty(name, value);
	}
}
