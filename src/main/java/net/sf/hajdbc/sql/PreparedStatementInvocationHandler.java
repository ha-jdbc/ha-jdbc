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

import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Map;
import java.util.Set;

import net.sf.hajdbc.Database;
import net.sf.hajdbc.invocation.Invoker;
import net.sf.hajdbc.util.reflect.Methods;

/**
 * @author Paul Ferraro
 * @param <D> 
 */
public class PreparedStatementInvocationHandler<Z, D extends Database<Z>> extends AbstractPreparedStatementInvocationHandler<Z, D, PreparedStatement>
{
	private static final Set<Method> setMethodSet = Methods.findMethods(PreparedStatement.class, "set\\w+");
	
	/**
	 * @param connection
	 * @param proxy
	 * @param invoker
	 * @param statementMap
	 * @param transactionContext 
	 * @param fileSupport 
	 * @param sql 
	 * @throws Exception
	 */
	public PreparedStatementInvocationHandler(Connection connection, SQLProxy<Z, D, Connection, SQLException> proxy, Invoker<Z, D, Connection, PreparedStatement, SQLException> invoker, Map<D, PreparedStatement> statementMap, TransactionContext<Z, D> transactionContext, FileSupport<SQLException> fileSupport, String sql) throws SQLException
	{
		super(connection, proxy, invoker, PreparedStatement.class, statementMap, transactionContext, fileSupport, setMethodSet);
		
		this.lockList = this.extractLocks(sql);
		this.selectForUpdate = this.isLockingSelect(sql);
	}
}
