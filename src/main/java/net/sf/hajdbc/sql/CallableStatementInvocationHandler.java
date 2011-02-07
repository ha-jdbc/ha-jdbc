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
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Map;
import java.util.Set;

import net.sf.hajdbc.Database;
import net.sf.hajdbc.invocation.InvocationStrategy;
import net.sf.hajdbc.invocation.InvocationStrategyEnum;
import net.sf.hajdbc.invocation.Invoker;
import net.sf.hajdbc.util.reflect.Methods;

/**
 * @author Paul Ferraro
 * @param <D> 
 */
@SuppressWarnings("nls")
public class CallableStatementInvocationHandler<Z, D extends Database<Z>> extends AbstractPreparedStatementInvocationHandler<Z, D, CallableStatement>
{
	private static final Set<Method> registerOutParameterMethodSet = Methods.findMethods(CallableStatement.class, "registerOutParameter");
	private static final Set<Method> setMethodSet = Methods.findMethods(CallableStatement.class, "set\\w+");
	private static final Set<Method> driverReadMethodSet = Methods.findMethods(CallableStatement.class, "get\\w+", "wasNull");
	{
		driverReadMethodSet.removeAll(Methods.findMethods(PreparedStatement.class, "get\\w+"));
	}
	
	/**
	 * @param connection
	 * @param proxy
	 * @param invoker
	 * @param statementMap
	 * @param transactionContext 
	 * @param fileSupport 
	 * @throws Exception
	 */
	public CallableStatementInvocationHandler(Connection connection, SQLProxy<Z, D, Connection, SQLException> proxy, Invoker<Z, D, Connection, CallableStatement, SQLException> invoker, Map<D, CallableStatement> statementMap, TransactionContext<Z, D> transactionContext, FileSupport<SQLException> fileSupport)
	{
		super(connection, proxy, invoker, CallableStatement.class, statementMap, transactionContext, fileSupport, setMethodSet);
	}

	/**
	 * @see net.sf.hajdbc.sql.AbstractStatementInvocationHandler#getInvocationStrategy(java.sql.Statement, java.lang.reflect.Method, java.lang.Object[])
	 */
	@Override
	protected InvocationStrategy getInvocationStrategy(CallableStatement statement, Method method, Object[] parameters) throws SQLException
	{
		if (registerOutParameterMethodSet.contains(method))
		{
			return InvocationStrategyEnum.INVOKE_ON_EXISTING;
		}
		
		if (driverReadMethodSet.contains(method))
		{
			return InvocationStrategyEnum.INVOKE_ON_ANY;
		}
		
		return super.getInvocationStrategy(statement, method, parameters);
	}

	/**
	 * @see net.sf.hajdbc.sql.AbstractPreparedStatementInvocationHandler#isBatchMethod(java.lang.reflect.Method)
	 */
	@Override
	protected boolean isBatchMethod(Method method)
	{
		return registerOutParameterMethodSet.contains(method) || super.isBatchMethod(method);
	}

	/**
	 * @see net.sf.hajdbc.sql.AbstractPreparedStatementInvocationHandler#isIndexType(java.lang.Class)
	 */
	@Override
	protected boolean isIndexType(Class<?> type)
	{
		return super.isIndexType(type) || type.equals(String.class);
	}
}
