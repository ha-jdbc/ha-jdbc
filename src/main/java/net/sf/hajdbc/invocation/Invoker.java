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
package net.sf.hajdbc.invocation;

import net.sf.hajdbc.Database;

/**
 * Represents a method invocation on a SQL object against a database.
 * @author Paul Ferraro
 * @param <D> Type of the root object (e.g. driver, datasource)
 * @param <T> Target object type of the invocation
 * @param <R> Return type of this invocation
 */
public interface Invoker<Z, D extends Database<Z>, T, R, E extends Exception>
{
	/**
	 * Invokes an action against the specified database on the specified SQL object
	 * @param database a database
	 * @param object an SQL object
	 * @return the invocation result
	 * @throws Exception
	 */
	R invoke(D database, T object) throws E;
}
