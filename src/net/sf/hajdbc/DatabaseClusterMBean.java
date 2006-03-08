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
	 */
	public void deactivate(String databaseId);

	/**
	 * Synchronizes, using the default strategy, and reactivates the specified database.
	 * @param databaseId a database identifier
	 * @throws Exception if activation fails
	 * @throws IllegalArgumentException if no database exists with the specified identifier.
	 */
	public void activate(String databaseId) throws Exception;

	/**
	 * Synchronizes, using the specified strategy, and reactivates the specified database.
	 * @param databaseId a database identifier
	 * @param syncId the class name of a synchronization strategy
	 * @throws Exception if activation fails
	 * @throws IllegalArgumentException if no database exists with the specified identifier, or no synchronization strategy exists with the specified identifier.
	 */
	public void activate(String databaseId, String syncId) throws Exception;
	
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
	
	public void addDatabase(String id, String url, String driver);
	
	public void addDatabase(String id, String name);
	
	public void removeDatabase(String id);
}
