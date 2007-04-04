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

import net.sf.hajdbc.DatabaseClusterMBean;

/**
 * @author Paul Ferraro
 *
 */
public interface DriverDatabaseClusterMBean extends DatabaseClusterMBean
{
	/**
	 * Adds a new database to this cluster using the specified identifier, url, and driver.
	 * @param databaseId a database identifier
	 * @param url a JDBC url
	 * @param driver a JDBC driver class name
	 * @throws IllegalArgumentException if this database already exists, if the specified driver is invalid, or if the specified url is invalid.
	 * @throws IllegalStateException if mbean registration fails.
	 */
	public void add(String databaseId, String driver, String url);
}
