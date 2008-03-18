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

import java.sql.Connection;
import java.sql.Statement;
import java.util.Map;

import net.sf.hajdbc.Database;

/**
 * @author Paul Ferraro
 * @param <D> 
 */
public class StatementInvocationHandler<D> extends AbstractStatementInvocationHandler<D, Statement>
{
	/**
	 * @param connection the connection that created this statement
	 * @param proxy the invocation handler of the connection that created this statement
	 * @param invoker the invoker used to create this statement
	 * @param statementMap a map of database to underlying statement
	 * @param transactionContext 
	 * @param fileSupport support for streams
	 * @throws Exception
	 */
	public StatementInvocationHandler(Connection connection, SQLProxy<D, Connection> proxy, Invoker<D, Connection, Statement> invoker, Map<Database<D>, Statement> statementMap, TransactionContext<D> transactionContext, FileSupport fileSupport) throws Exception
	{
		super(connection, proxy, invoker, Statement.class, statementMap, transactionContext, fileSupport);
	}
}
