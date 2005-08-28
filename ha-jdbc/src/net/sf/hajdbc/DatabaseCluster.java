/*
 * HA-JDBC: High-Availability JDBC
 * Copyright (C) 2004 Paul Ferraro
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

import java.sql.SQLException;


/**
 * @author  Paul Ferraro
 * @since   1.0
 */
public interface DatabaseCluster extends DatabaseClusterMBean
{
	/**
	 * Activates the specified database
	 * @param database a database descriptor
	 * @return true, if the database was activated, false it was already active
	 */
	public boolean activate(Database database);
	
	/**
	 * Deactivates the specified database
	 * @param database a database descriptor
	 * @return true, if the database was deactivated, false it was already inactive
	 */
	public boolean deactivate(Database database);
	
	/**
	 * Returns a connection factory proxy for this obtaining connections to databases in this cluster.
	 * @return a connection factory proxy
	 */
	public ConnectionFactory getConnectionFactory();
	
	/**
	 * Determines whether or not the specified database is responding
	 * @param database a database descriptor
	 * @return true, if the database is responding, false if it appears down
	 */
	public boolean isAlive(Database database);
	
	/**
	 * Returns the database identified by the specified id
	 * @param id a database identifier
	 * @return a database descriptor
	 * @throws SQLException if no database exists with the specified identifier
	 */
	public Database getDatabase(String id) throws SQLException;
	
	/**
	 * Initializes this database cluster.
	 * @throws SQLException if initialization fails
	 */
	public void init() throws SQLException;

	/**
	 * Loads the persisted state of this database cluster
	 * @return an array of database identifiers
	 * @throws SQLException if state could not be obtained
	 */
	public String[] loadState() throws SQLException;
	
	/**
	 * Returns the default synchronization strategy for this database cluster
	 * @return a synchronization strategy implementation
	 */
	public SynchronizationStrategy getDefaultSynchronizationStrategy();

	/**
	 * Handles a failure caused by the specified cause on the specified database.
	 * If the database is not alive, then it is deactivated, otherwise an exception is thrown back to the caller.
	 * @param database a database descriptor
	 * @param cause the cause of the failure
	 * @throws SQLException if the database is alive
	 */
	public void handleFailure(Database database, SQLException cause) throws SQLException;
	
	/**
	 * Returns the Balancer implementation used by this database cluster.
	 * @return a Balancer implementation
	 */
	public Balancer getBalancer();
}
