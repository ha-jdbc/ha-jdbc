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

/**
 * @author  Paul Ferraro
 * @since   1.0
 */
public interface Balancer
{
	/**
	 * Removes the specified database from this balancer.
	 * @param database a database descriptor
	 * @return true, if the database was removed successfully, false if it did not exist.
	 */
	public boolean remove(Database database);

	/**
	 * Adds the specified database to this balancer.
	 * @param database a database descriptor
	 * @return true, if the database was added successfully, false if already existed.
	 */
	public boolean add(Database database);

	/**
	 * Returns the first database from this balancer
	 * @return the first database from this balancer
	 */
	public Database first();

	/**
	 * Returns the next database from this balancer
	 * @return the next database from this balancer
	 */
	public Database next();

	/**
	 * Returns the databases known to this balancer
	 * @return an array of database descriptors
	 */
	public Database[] toArray();

	/**
	 * Check whether the specified database is known to this balancer
	 * @param database a database descriptor
	 * @return true, if the database is known to this balancer, false otherwise
	 */
	public boolean contains(Database database);
	
	/**
	 * Executes the specifed operation on the specified object of the specified database.
	 * @param operation a database operation
	 * @param database a database descriptor
	 * @param object a SQL object
	 * @return the result of the operation
	 * @throws java.sql.SQLException if the operation fails
	 */
	public Object execute(Operation operation, Database database, Object object) throws java.sql.SQLException;
}