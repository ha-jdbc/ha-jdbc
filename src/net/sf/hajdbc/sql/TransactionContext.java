/*
 * HA-JDBC: High-Availability JDBC
 * Copyright (c) 2004-2008 Paul Ferraro
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

import java.sql.Connection;
import java.sql.SQLException;

/**
 * Decorates and invocation strategy with transaction boundary logic.
 * @author Paul Ferraro
 * @param <D> DataSource or Driver
 */
public interface TransactionContext<D>
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
	public <T, R> InvocationStrategy<D, T, R> start(InvocationStrategy<D, T, R> strategy, Connection connection) throws SQLException;

	/**
	 * Decorates the specified invocation strategy with end transaction logic.
	 * @param <T> Target object type of the invocation
	 * @param <R> Return type of this invocation
	 * @param strategy
	 * @return the decorated invocation strategy
	 * @throws SQLException
	 */
	public <T, R> InvocationStrategy<D, T, R> end(InvocationStrategy<D, T, R> strategy) throws SQLException;
	
	/**
	 * Closes this transaction context.
	 */
	public void close();
}