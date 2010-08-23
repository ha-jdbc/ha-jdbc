/*
 * HA-JDBC: High-Availability JDBC
 * Copyright 2004-2009 Paul Ferraro
 * 
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 * 
 * You should have received a copy of the GNU Lesser General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package net.sf.hajdbc.sql;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlType;

import net.sf.hajdbc.Database;
import net.sf.hajdbc.codec.Codec;
import net.sf.hajdbc.management.Description;
import net.sf.hajdbc.management.ManagedAttribute;
import net.sf.hajdbc.management.ManagedOperation;
import net.sf.hajdbc.sql.AbstractDatabaseClusterConfiguration.Property;

/**
 * @author  Paul Ferraro
 * @param <D> either java.sql.Driver or javax.sql.DataSource
 * @since   1.0
 */
@XmlType(propOrder = { "name", "xmlProperties", "user", "password" })
public abstract class AbstractDatabase<Z> implements Database<Z>
{
	@XmlAttribute(name = "id", required = true)
	private String id;
	@XmlElement(name = "name", required = true)
	private String name;
	@XmlElement(name = "user")
	private String user;
	@XmlElement(name = "password")
	private String password;

	@XmlAttribute(name = "weight")
	private Integer weight = 1;
	@XmlAttribute(name = "local")
	private Boolean local = false;

	private Map<String, String> properties = new HashMap<String, String>();
	private boolean dirty = false;
	private volatile boolean active = false;
	
	@SuppressWarnings("unused")
	@XmlElement(name = "property")
	private Property[] getXmlProperties()
	{
		List<Property> properties = new ArrayList<Property>(this.properties.size());
		
		for (Map.Entry<String, String> entry: this.properties.entrySet())
		{
			Property property = new Property();
			property.setName(entry.getKey());
			property.setValue(entry.getValue());
			properties.add(property);
		}
		
		return properties.toArray(new Property[properties.size()]);
	}
	
	@SuppressWarnings("unused")
	private void setXmlProperties(Property[] properties)
	{
		for (Property property: properties)
		{
			this.properties.put(property.getName(), property.getValue());
		}
	}
	
	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.ActiveDatabaseMBean#getDatabaseId()
	 */
	@ManagedAttribute
	@Description("Uniquely identifies this database in the cluster")
	@Override
	public String getId()
	{
		return this.id;
	}

	public void setId(String id)
	{
		this.id = id;
	}
	
	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.Database#getName()
	 */
	@ManagedAttribute
	@Description("Identifies the location of this database")
	@Override
	public String getName()
	{
		return this.name;
	}

	@ManagedAttribute
	public void setName(String name)
	{
		this.assertInactive();
		this.checkDirty(this.name, name);
		this.name = name;
	}
	
	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.ActiveDatabaseMBean#getUser()
	 */
	@ManagedAttribute
	@Description("User ID for administrative connection authentication")
	public String getUser()
	{
		return this.user;
	}
	
	/**
	 * @see net.sf.hajdbc.InactiveDatabaseMBean#setUser(java.lang.String)
	 */
	@ManagedAttribute
	public void setUser(String user)
	{
		this.assertInactive();
		this.checkDirty(this.user, user);
		this.user = user;
	}
	
	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.ActiveDatabaseMBean#getPassword()
	 */
	@ManagedAttribute
	@Description("Password for administrative connection authentication")
	public String getPassword()
	{
		return this.password;
	}
	
	/**
	 * @see net.sf.hajdbc.InactiveDatabaseMBean#setPassword(java.lang.String)
	 */
	@ManagedAttribute
	public void setPassword(String password)
	{
		this.assertInactive();
		this.checkDirty(this.password, password);
		this.password = password;
	}

	/**
	 * {@inheritDoc}
	 * @throws SQLException 
	 * @see net.sf.hajdbc.Database#decodePassword(net.sf.hajdbc.codec.Codec)
	 */
	@Override
	public String decodePassword(Codec codec) throws SQLException
	{
		return (this.password != null) ? codec.decode(this.password) : null;
	}

	/**
	 * @see net.sf.hajdbc.ActiveDatabaseMBean#getWeight()
	 */
	@ManagedAttribute
	@Description("Weight used in read request balancing")
	@Override
	public int getWeight()
	{
		return this.weight;
	}
	
	/**
	 * @see net.sf.hajdbc.InactiveDatabaseMBean#setWeight(int)
	 */
	@ManagedAttribute
	public void setWeight(int weight)
	{
		this.assertInactive();
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
	@Override
	public boolean equals(Object object)
	{
		if ((object == null) || !(object instanceof Database<?>)) return false;
		
		String id = ((Database<?>) object).getId();
		
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
	 * @see java.lang.Comparable#compareTo(Object)
	 */
	@Override
	public int compareTo(Database<Z> database)
	{
		return this.id.compareTo(database.getId());
	}
	
	/**
	 * @see net.sf.hajdbc.ActiveDatabaseMBean#getProperties()
	 */
	@ManagedAttribute
	@Description("Connection properties")
	public Map<String, String> getProperties()
	{
		return this.properties;
	}

	/**
	 * @see net.sf.hajdbc.InactiveDatabaseMBean#removeProperty(java.lang.String)
	 */
	@ManagedOperation
	@Description("Removes the specified connection property")
	public void removeProperty(String name)
	{
		this.assertInactive();
		
		String value = this.properties.remove(name);
		
		this.dirty |= (value != null);
	}

	/**
	 * @see net.sf.hajdbc.InactiveDatabaseMBean#setProperty(java.lang.String, java.lang.String)
	 */
	@ManagedOperation
	@Description("Creates/updates the specified connection property")
	public void setProperty(String name, String value)
	{
		this.assertInactive();
		
		if ((name == null) || (value == null))
		{
			throw new IllegalArgumentException();
		}
		
		String old = this.properties.put(name, value);
		
		this.checkDirty(old, value);
	}

	/**
	 * @see net.sf.hajdbc.InactiveDatabaseMBean#setLocal(boolean)
	 */
	@ManagedAttribute
	public void setLocal(boolean local)
	{
		this.assertInactive();
		this.checkDirty(this.local, local);
		this.local = local;
	}

	/**
	 * @see net.sf.hajdbc.ActiveDatabaseMBean#isLocal()
	 */
	@ManagedAttribute
	@Description("Indicates whether this database is local to this JVM")
	@Override
	public boolean isLocal()
	{
		return this.local;
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
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.Database#isActive()
	 */
	@ManagedAttribute
	@Description("Indicates whether or not this database is active")
	@Override
	public boolean isActive()
	{
		return this.active;
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.Database#setActive(boolean)
	 */
	@Override
	public void setActive(boolean active)
	{
		this.active = active;
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
	 * Helper method to determine whether the connect() method requires authentication.
	 * @return true, if authentication is required, false otherwise
	 */
	protected boolean requiresAuthentication()
	{
		return this.user != null;
	}
	
	protected void assertInactive()
	{
		if (this.active)
		{
			throw new IllegalStateException();
		}
	}
}
