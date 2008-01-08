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

import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.Map;
import java.util.Set;

import net.sf.hajdbc.Database;

/**
 * @author Paul Ferraro
 *
 */
public class PreparedStatementInvocationHandler<D> extends AbstractPreparedStatementInvocationHandler<D, PreparedStatement>
{
	protected Set<String> identifierSet;
	protected boolean selectForUpdate;
	
	/**
	 * @param connection
	 * @param proxy
	 * @param invoker
	 * @param statementMap
	 * @throws Exception
	 */
	public PreparedStatementInvocationHandler(Connection connection, SQLProxy<D, Connection> proxy, Invoker<D, Connection, PreparedStatement> invoker, Map<Database<D>, PreparedStatement> statementMap, FileSupport fileSupport, String sql) throws Exception
	{
		super(connection, proxy, invoker, PreparedStatement.class, statementMap, fileSupport);

		this.identifierSet = this.extractIdentifiers(sql);
		this.selectForUpdate = this.isSelectForUpdate(sql);
	}

	/**
	 * @see net.sf.hajdbc.sql.AbstractStatementInvocationHandler#getInvocationStrategy(java.sql.Statement, java.lang.reflect.Method, java.lang.Object[])
	 */
	@SuppressWarnings("nls")
	@Override
	protected InvocationStrategy<D, PreparedStatement, ?> getInvocationStrategy(PreparedStatement statement, Method method, Object[] parameters) throws Exception
	{
		if (method.equals(PreparedStatement.class.getMethod("execute")) || method.equals(PreparedStatement.class.getMethod("executeUpdate")))
		{
			return new TransactionalDatabaseWriteInvocationStrategy<D, PreparedStatement, Object>(this.identifierSet);
		}
		
		if (method.equals(PreparedStatement.class.getMethod("executeQuery")))
		{
			return (this.identifierSet.isEmpty() && !this.selectForUpdate && (statement.getResultSetConcurrency() == java.sql.ResultSet.CONCUR_READ_ONLY)) ? new DatabaseReadInvocationStrategy<D, PreparedStatement, Object>() : new EagerResultSetInvocationStrategy<D, PreparedStatement>(statement, this.fileSupport, this.identifierSet);
		}
		
		return super.getInvocationStrategy(statement, method, parameters);
	}
}
