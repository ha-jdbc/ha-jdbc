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

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collection;
import java.util.concurrent.ExecutorService;


/**
 * @author Paul Ferraro
 * @since 1.2
 */
public interface SynchronizationContext
{
	/**
	 * Returns a connection to the specified database.
	 * @param database a database to which to connect
	 * @return a database connection
	 * @throws SQLException if connection could not be obtained
	 */
	public Connection getConnection(Database database) throws SQLException;
	
	/**
	 * Returns the database from which to synchronize.
	 * @return a database
	 */
	public Database getSourceDatabase();
	
	/**
	 * Returns the database to synchronize.
	 * @return a database
	 */
	public Database getTargetDatabase();
	
	/**
	 * Returns a snapshot of the activate databases in the cluster at the time synchronization started.
	 * @return a collection of databases
	 */
	public Collection<Database> getActiveDatabases();
	
	/**
	 * Returns a cache of database meta data.
	 * @return a cache of database meta data.
	 */
	public DatabaseMetaDataCache getDatabaseMetaDataCache();
	
	/**
	 * Returns the dialect of the cluster.
	 * @return a dialect
	 */
	public Dialect getDialect();
	
	/**
	 * An executor service for executing tasks asynchronously.
	 * @return an executor service
	 */
	public ExecutorService getExecutor();
	
	/**
	 * Closes any open database connections and shuts down the executor service. 
	 */
	public void close();
}