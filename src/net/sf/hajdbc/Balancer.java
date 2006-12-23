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

import java.util.Set;

/**
 * @author  Paul Ferraro
 * @since   1.0
 * @param <D> either java.sql.Driver or javax.sql.DataSource
 */
public interface Balancer<D>
{
	/**
	 * Removes the specified database from this balancer.
	 * @param database a database descriptor
	 * @return true, if the database was removed successfully, false if it did not exist.
	 */
	public boolean remove(Database<D> database);

	/**
	 * Adds the specified database to this balancer.
	 * @param database a database descriptor
	 * @return true, if the database was added successfully, false if already existed.
	 */
	public boolean add(Database<D> database);

	/**
	 * Returns the first database from this balancer
	 * @return the first database from this balancer
	 */
	public Database<D> first();

	/**
	 * Returns the next database from this balancer
	 * @return the next database from this balancer
	 */
	public Database<D> next();

	/**
	 * Returns an unmodifiable collection of databases known to this balancer
	 * @return a collection of database descriptors
	 */
	public Set<Database<D>> all();

	/**
	 * Check whether the specified database is known to this balancer
	 * @param database a database descriptor
	 * @return true, if the database is known to this balancer, false otherwise
	 */
	public boolean contains(Database<D> database);
	
	/**
	 * Called before an operation is performed on the specified database retrieved via {@link #next()}.
	 * @param database a database descriptor
	 */
	public void beforeOperation(Database<D> database);
	
	/**
	 * Called after an operation is performed on the specified database retrieved via {@link #next()}.
	 * @param database a database descriptor
	 */
	public void afterOperation(Database<D> database);
}