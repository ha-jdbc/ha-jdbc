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
package net.sf.hajdbc.sql;

import net.sf.hajdbc.Database;

/**
 * Defines a strategy for invoking an invoker.
 * @author Paul Ferraro
 * @param <D> Type of the root object (e.g. driver, datasource)
 * @param <T> Target object type of the invocation
 * @param <R> Return type of this invocation
 */
public interface InvocationStrategy<Z, D extends Database<Z>, T, R, E extends Exception>
{
	/**
	 * Invoke the specified invoker against the specified proxy.
	 * @param proxy a JDBC object proxy
	 * @param invoker an invoker
	 * @return a result
	 * @throws Exception if the invocation fails
	 */
	R invoke(SQLProxy<Z, D, T, E> proxy, Invoker<Z, D, T, R, E> invoker) throws E;
}
