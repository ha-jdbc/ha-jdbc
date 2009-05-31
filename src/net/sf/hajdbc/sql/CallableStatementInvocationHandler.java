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
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.Map;
import java.util.Set;

import net.sf.hajdbc.Database;
import net.sf.hajdbc.util.reflect.Methods;

/**
 * @author Paul Ferraro
 * @param <D> 
 */
@SuppressWarnings("nls")
public class CallableStatementInvocationHandler<D> extends AbstractPreparedStatementInvocationHandler<D, CallableStatement>
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
	public CallableStatementInvocationHandler(Connection connection, SQLProxy<D, Connection> proxy, Invoker<D, Connection, CallableStatement> invoker, Map<Database<D>, CallableStatement> statementMap, TransactionContext<D> transactionContext, FileSupport fileSupport) throws Exception
	{
		super(connection, proxy, invoker, CallableStatement.class, statementMap, transactionContext, fileSupport);
	}

	/**
	 * @see net.sf.hajdbc.sql.AbstractStatementInvocationHandler#getInvocationStrategy(java.sql.Statement, java.lang.reflect.Method, java.lang.Object[])
	 */
	@Override
	protected InvocationStrategy<D, CallableStatement, ?> getInvocationStrategy(CallableStatement statement, Method method, Object[] parameters) throws Exception
	{
		if (registerOutParameterMethodSet.contains(method) || setMethodSet.contains(method))
		{
			return new DriverWriteInvocationStrategy<D, CallableStatement, Object>();
		}
		
		if (driverReadMethodSet.contains(method))
		{
			return new DriverReadInvocationStrategy<D, CallableStatement, Object>();
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
		return type.equals(String.class) || super.isIndexType(type);
	}
}
