/*
 * HA-JDBC: High-Availability JDBC
 * Copyright (C) 2012  Paul Ferraro
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
package net.sf.hajdbc.sync;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Set;
import java.util.concurrent.ExecutorService;

import net.sf.hajdbc.Database;
import net.sf.hajdbc.DatabaseProperties;
import net.sf.hajdbc.codec.Decoder;
import net.sf.hajdbc.dialect.Dialect;


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
	Connection getConnection(D database) throws SQLException;
	
	/**
	 * Returns the database from which to synchronize.
	 * @return a database
	 */
	D getSourceDatabase();
	
	/**
	 * Returns the database to synchronize.
	 * @return a database
	 */
	D getTargetDatabase();
	
	/**
	 * Returns a snapshot of the activate databases in the cluster at the time synchronization started.
	 * @return a collection of databases
	 */
	Set<D> getActiveDatabaseSet();
	
	/**
	 * Returns a cache of database meta data for the source database.
	 * @return a cache of database meta data
	 */
	DatabaseProperties getSourceDatabaseProperties();
	
	/**
	 * Returns a cache of database meta data for the target database.
	 * @return a cache of database meta data
	 */
	DatabaseProperties getTargetDatabaseProperties();
	
	/**
	 * Returns the dialect of the cluster.
	 * @return a dialect
	 */
	Dialect getDialect();
	
	/**
	 * An executor service for executing tasks asynchronously.
	 * @return an executor service
	 */
	ExecutorService getExecutor();
	
	SynchronizationSupport getSynchronizationSupport();
	
	Decoder getDecoder();
	
	/**
	 * Closes any open database connections and shuts down the executor service. 
	 */
	void close();
}
