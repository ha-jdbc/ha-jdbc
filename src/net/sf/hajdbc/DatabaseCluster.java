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

import java.sql.SQLException;
import java.util.Map;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.locks.Lock;


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
	 * Returns a map of database to connection factory for this obtaining connections to databases in this cluster.
	 * @return a connection factory map
	 */
	public Map<Database, ?> getConnectionFactoryMap();
	
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
	 * @throws IllegalArgumentException if no database exists with the specified identifier
	 */
	public Database getDatabase(String id);
	
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
	 * @return an implementation of <code>Balancer</code>
	 */
	public Balancer getBalancer();
	
	/**
	 * Returns an executor service used to asynchronously execute database writes.
	 * @return an implementation of <code>ExecutorService</code>
	 * @since 1.1
	 */
	public ExecutorService getExecutor();
	
	/**
	 * Returns a dialect capable of returning database vendor specific values.
	 * @return an implementation of <code>Dialect</code>
	 * @since 1.1
	 */
	public Dialect getDialect();
	
	/**
	 * Returns a Lock that can acquire a read lock on this database cluster.
	 * @return a read lock
	 * @since 1.1
	 */
	public Lock readLock();
	
	/**
	 * Returns a Lock that can acquire a write lock on this database cluster.
	 * @return a write lock
	 * @since 1.1
	 */
	public Lock writeLock();
	
	/**
	 * Starts this database cluster.
	 * @throws SQLException if database cluster fails to start
	 * @since 1.1
	 */
	public void start() throws SQLException;
	
	/**
	 * Stops this database cluster.
	 * @since 1.1
	 */
	public void stop();
}
