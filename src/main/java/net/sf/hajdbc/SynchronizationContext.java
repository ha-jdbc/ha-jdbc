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
package net.sf.hajdbc;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Set;
import java.util.concurrent.ExecutorService;

import net.sf.hajdbc.cache.DatabaseProperties;


/**
 * @author Paul Ferraro
 * @param <D> Driver or DataSource
 * @since 2.0
 */
public interface SynchronizationContext<Z, D extends Database<Z>>
{
	/**
	 * Returns a connection to the specified database.
	 * @param database a database to which to connect
	 * @return a database connection
	 * @throws SQLException if connection could not be obtained
	 */
	public Connection getConnection(D database) throws SQLException;
	
	/**
	 * Returns the database from which to synchronize.
	 * @return a database
	 */
	public D getSourceDatabase();
	
	/**
	 * Returns the database to synchronize.
	 * @return a database
	 */
	public D getTargetDatabase();
	
	/**
	 * Returns a snapshot of the activate databases in the cluster at the time synchronization started.
	 * @return a collection of databases
	 */
	public Set<D> getActiveDatabaseSet();
	
	/**
	 * Returns a cache of database meta data for the source database.
	 * @return a cache of database meta data
	 */
	public DatabaseProperties getSourceDatabaseProperties();
	
	/**
	 * Returns a cache of database meta data for the target database.
	 * @return a cache of database meta data
	 */
	public DatabaseProperties getTargetDatabaseProperties();
	
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
