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
package net.sf.hajdbc;

/**
 * @author  Paul Ferraro
 * @since   1.1
 */
public interface InactiveDatabaseMBean extends ActiveDatabaseMBean
{
	/**
	 * Set the weight for this database
	 * @param weight a weight
	 * @exception IllegalArgumentException if weight is less than 0
	 */
	public void setWeight(int weight);
	
	/**
	 * Set the user for this database.
	 * @param user a user
	 */
	public void setUser(String user);
	
	/**
	 * Set the password for this database
	 * @param password a password
	 */
	public void setPassword(String password);
	
	/**
	 * Set the value of the specified property.
	 * @param name a property name
	 * @param value a property value
	 */
	public void setProperty(String name, String value);
	
	/**
	 * Removes the specified property.
	 * @param name a property name
	 */
	public void removeProperty(String name);
}
