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
package net.sf.hajdbc;

import java.sql.Connection;
import java.sql.SQLException;

import net.sf.hajdbc.codec.Decoder;

/**
 * @author  Paul Ferraro
 * @version $Revision: 1536 $
 * @param <Z> connection source (e.g. Driver, DataSource, etc.)
 * @since   1.0
 */
public interface Database<Z> extends Comparable<Database<Z>>
{
	static final int ID_MAX_SIZE = 64;
	
	/**
	 * Returns the unique idenfier for this database
	 * @return a unique identifier
	 */
	String getId();
	
	/**
	 * Returns the location of this database
	 * @return a location
	 */
	String getLocation();
	
	/**
	 * Returns the relative "weight" of this cluster node.
	 * In general, when choosing a node to service read requests, a cluster will favor the node with the highest weight.
	 * A weight of 0 is somewhat special, depending on the type of balancer used by the cluster.
	 * In most cases, a weight of 0 means that this node will never service read requests unless it is the only node in the cluster.
	 * @return a positive integer
	 */
	int getWeight();
	
	/**
	 * Indicates whether or not this database is local to the machine on which the JVM resides.
	 * @return true if local, false if remote
	 */
	boolean isLocal();

	String decodePassword(Decoder decoder) throws SQLException;
	
	/**
	 * Connects to the database using the specified connection factory.
	 * @param connectionSource a factory object for creating connections
	 * @param password a decoded password
	 * @return a database connection
	 * @throws SQLException if connection fails
	 */
	Connection connect(Z connectionSource, String password) throws SQLException;
	
	/**
	 * Returns a connection source for this database.
	 * @return a connection source
	 * @throws IllegalArgumentException if connection source could not be created
	 */
	Z getConnectionSource();
	
	boolean isDirty();
	
	void clean();
	
	boolean isActive();
	
	void setActive(boolean active);
}
