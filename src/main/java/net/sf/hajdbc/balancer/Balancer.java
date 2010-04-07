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
package net.sf.hajdbc.balancer;

import java.util.Set;

import net.sf.hajdbc.Database;

/**
 * The set of active databases and a strategy for determining the next balancing target.
 * 
 * @author Paul Ferraro
 *
 * @param <Z> the database connection source
 * @param <D> the database descriptor
 */
public interface Balancer<Z, D extends Database<Z>> extends Set<D>
{
	/**
	 * Returns the master database.
	 * The database returned by this method should be consistent across nodes.
	 * @return the master database
	 */
	D master();
	
	/**
	 * The non-master databases.
	 * @return an iterable collection of databases.
	 */
	Iterable<D> slaves();
	
	/**
	 * Returns the next database from this balancer
	 * @return the next database from this balancer
	 */
	D next();

	/**
	 * Called before an operation is performed on the specified database retrieved via {@link #next()}.
	 * @param database a database descriptor
	 */
	void beforeInvocation(D database);
	
	/**
	 * Called after an operation is performed on the specified database retrieved via {@link #next()}.
	 * @param database a database descriptor
	 */
	void afterInvocation(D database);
}
