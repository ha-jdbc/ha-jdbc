/*
 * HA-JDBC: High-Availability JDBC
 * Copyright (c) 2004-2007 Paul Ferraro
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
package net.sf.hajdbc.sql;

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
	public R invoke(D database, T object) throws E;
}