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
package net.sf.hajdbc;

import java.util.Collection;

/**
 * @author  Paul Ferraro
 * @version $Revision$
 * @since   1.0
 */
public interface DatabaseClusterMBean
{
	/**
	 * Returns the name of this database cluster.
	 * @return the database cluster name
	 */
	public String getId();
	
	/**
	 * Determines whether or not the specified database is responsive
	 * @param databaseId a database identifier
	 * @return true, if the database is alive, false otherwise
	 * @throws IllegalArgumentException if no database exists with the specified identifier.
	 */
	public boolean isAlive(String databaseId);
	
	/**
	 * Deactivates the specified database.
	 * @param databaseId a database identifier
	 * @throws IllegalArgumentException if no database exists with the specified identifier.
	 * @throws IllegalStateException if mbean could not be re-registered using inactive database interface.
	 */
	public void deactivate(String databaseId);

	/**
	 * Synchronizes, using the default strategy, and reactivates the specified database.
	 * @param databaseId a database identifier
	 * @throws IllegalArgumentException if no database exists with the specified identifier.
	 * @throws IllegalStateException if synchronization fails, or if mbean could not be re-registered using active database interface.
	 */
	public void activate(String databaseId);

	/**
	 * Synchronizes, using the specified strategy, and reactivates the specified database.
	 * @param databaseId a database identifier
	 * @param syncId the class name of a synchronization strategy
	 * @throws IllegalArgumentException if no database exists with the specified identifier, or no synchronization strategy exists with the specified identifier.
	 * @throws IllegalStateException if synchronization fails, or if mbean could not be re-registered using active database interface.
	 */
	public void activate(String databaseId, String syncId);
	
	/**
	 * Returns a collection of active databases in this cluster.
	 * @return a list of database identifiers
	 */
	public Collection<String> getActiveDatabases();
	
	/**
	 * Returns a collection of inactive databases in this cluster.
	 * @return a collection of database identifiers
	 */
	public Collection<String> getInactiveDatabases();
	
	/**
	 * Return the current HA-JDBC version
	 * @return the current version
	 */
	public String getVersion();
	
	/**
	 * Adds a new database to this cluster using the specified identifier, url, and driver.
	 * @param databaseId a database identifier
	 * @param url a JDBC url
	 * @param driver a JDBC driver class name
	 * @throws IllegalArgumentException if this database already exists, if the specified driver is invalid, or if the specified url is invalid.
	 * @throws IllegalStateException if mbean registration fails.
	 */
	public void add(String databaseId, String driver, String url);
	
	/**
	 * Adds a new DataSource to this cluster using the specified identifier and JNDI name.
	 * @param databaseId a database identifier
	 * @param name the JNDI name use to lookup the DataSource
	 * @throws IllegalArgumentException if this database already exists, or no DataSource was found using the specified name.
	 * @throws IllegalStateException if mbean registration fails.
	 */
	public void add(String databaseId, String name);
	
	/**
	 * Removes the specified database/DataSource from the cluster.
	 * @param databaseId a database identifier
	 * @throws IllegalStateException if database is still active, or if mbean unregistration fails.
	 */
	public void remove(String databaseId);
	
	/**
	 * Flushes this cluster's cache of DatabaseMetaData.
	 */
	public void flushMetaDataCache();
}
