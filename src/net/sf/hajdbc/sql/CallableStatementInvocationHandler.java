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

import net.sf.hajdbc.Database;

/**
 * @author Paul Ferraro
 *
 */
public class CallableStatementInvocationHandler<D> extends AbstractPreparedStatementInvocationHandler<D, CallableStatement>
{
	/**
	 * @param connection
	 * @param proxy
	 * @param invoker
	 * @param statementMap
	 * @throws Exception
	 */
	public CallableStatementInvocationHandler(Connection connection, SQLProxy<D, Connection> proxy, Invoker<D, Connection, CallableStatement> invoker, Map<Database<D>, CallableStatement> statementMap, FileSupport fileSupport, String sql) throws Exception
	{
		super(connection, proxy, invoker, CallableStatement.class, statementMap, fileSupport, sql);
	}

	/**
	 * @see net.sf.hajdbc.sql.AbstractStatementInvocationHandler#getInvocationStrategy(java.sql.Statement, java.lang.reflect.Method, java.lang.Object[])
	 */
	@Override
	protected InvocationStrategy<D, CallableStatement, ?> getInvocationStrategy(CallableStatement object, Method method, Object[] parameters) throws Exception
	{
		String methodName = method.getName();
		
		if (methodName.equals("registerOutParameter"))
		{
			return new DriverWriteInvocationStrategy<D, CallableStatement, Object>();
		}
		
		Class<?>[] types = method.getParameterTypes();
		
		if (methodName.startsWith("get") && (types != null) && (types.length > 0) && ((types[0].equals(Integer.TYPE) || types[0].equals(String.class))))
		{
			if (!method.equals(PreparedStatement.class.getMethod("getMoreResults", Integer.TYPE)))
			{
				return new DriverReadInvocationStrategy<D, CallableStatement, Object>();
			}
		}
		
		if (method.equals(CallableStatement.class.getMethod("wasNull")))
		{
			return new DriverReadInvocationStrategy<D, CallableStatement, Object>();
		}
		
		return super.getInvocationStrategy(object, method, parameters);
	}
}
