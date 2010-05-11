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

import java.sql.Connection;
import java.sql.SQLException;

import net.sf.hajdbc.Database;
import net.sf.hajdbc.durability.Durability;

/**
 * Decorates an invocation strategy with transaction boundary logic.
 * @author Paul Ferraro
 * @param <D> DataSource or Driver
 */
public interface TransactionContext<Z, D extends Database<Z>>
{
	/**
	 * Decorates the specified invocation strategy with start transaction logic.
	 * @param <T> Target object type of the invocation
	 * @param <R> Return type of this invocation
	 * @param strategy
	 * @param connection
	 * @return the decorated invocation strategy
	 * @throws SQLException
	 */
	InvocationStrategy start(InvocationStrategy strategy, Connection connection) throws SQLException;

	<T, R> Invoker<Z, D, T, R, SQLException> start(Invoker<Z, D, T, R, SQLException> invoker, Connection connection) throws SQLException;

	/**
	 * Decorates the specified invocation strategy with end transaction logic.
	 * @param <T> Target object type of the invocation
	 * @param <R> Return type of this invocation
	 * @param strategy
	 * @return the decorated invocation strategy
	 * @throws SQLException
	 */
	InvocationStrategy end(InvocationStrategy strategy, Durability.Phase phase) throws SQLException;

	<T, R> Invoker<Z, D, T, R, SQLException> end(Invoker<Z, D, T, R, SQLException> invoker, Durability.Phase phase) throws SQLException;
	
	/**
	 * Closes this transaction context.
	 */
	void close();
}
