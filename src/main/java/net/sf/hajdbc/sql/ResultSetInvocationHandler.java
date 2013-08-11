/*
 * HA-JDBC: High-Availability JDBC
 * Copyright (C) 2012  Paul Ferraro
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
import java.sql.Clob;
import java.sql.NClob;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Set;

import net.sf.hajdbc.Database;
import net.sf.hajdbc.invocation.InvocationStrategies;
import net.sf.hajdbc.invocation.InvocationStrategy;
import net.sf.hajdbc.invocation.Invoker;
import net.sf.hajdbc.util.reflect.Methods;

/**
 * @author Paul Ferraro
 * @param <D> 
 * @param <S> 
 */
@SuppressWarnings("nls")
public class ResultSetInvocationHandler<Z, D extends Database<Z>, S extends Statement> extends InputSinkRegistryInvocationHandler<Z, D, S, ResultSet, ResultSetProxyFactory<Z, D, S>>
{
	private static final Set<Method> driverReadMethodSet = Methods.findMethods(ResultSet.class, "findColumn", "getConcurrency", "getCursorName", "getFetchDirection", "getFetchSize", "getHoldability", "getMetaData", "getRow", "getType", "getWarnings", "isAfterLast", "isBeforeFirst", "isClosed", "isFirst", "isLast", "row(Deleted|Inserted|Updated)", "wasNull");
	private static final Set<Method> driverWriteMethodSet = Methods.findMethods(ResultSet.class, "absolute", "afterLast", "beforeFirst", "cancelRowUpdates", "clearWarnings", "first", "last", "moveTo(Current|Insert)Row", "next", "previous", "relative", "setFetchDirection", "setFetchSize");
	private static final Set<Method> transactionalWriteMethodSet = Methods.findMethods(ResultSet.class, "(delete|insert|update)Row");
	private static final Set<Method> getArrayMethodSet = Methods.findMethods(ResultSet.class, "getArray");
	private static final Set<Method> getBlobMethodSet = Methods.findMethods(ResultSet.class, "getBlob");
	private static final Set<Method> getClobMethodSet = Methods.findMethods(ResultSet.class, "getClob");
	private static final Set<Method> getNClobMethodSet = Methods.findMethods(ResultSet.class, "getNClob");
	private static final Set<Method> getRefMethodSet = Methods.findMethods(ResultSet.class, "getRef");
	private static final Set<Method> getSQLXMLMethodSet = Methods.findMethods(ResultSet.class, "getSQLXML");

	private static final Method closeMethod = Methods.getMethod(ResultSet.class, "close");
	private static final Method getStatementMethod = Methods.getMethod(ResultSet.class, "getStatement");
	
	/**
	 * @param statement the statement that created this result set
	 * @param proxy the invocation handler of the statement that created this result set
	 * @param invoker the invoker that was used to create this result set
	 * @param resultSetMap a map of database to underlying result set
	 * @param transactionContext 
	 * @param fileSupport support for streams
	 * @throws Exception
	 */
	public ResultSetInvocationHandler(ResultSetProxyFactory<Z, D, S> factory)
	{
		super(ResultSet.class, factory, getStatementMethod);
	}

	@Override
	protected ProxyFactoryFactory<Z, D, ResultSet, SQLException, ?, ? extends Exception> getProxyFactoryFactory(ResultSet object, Method method, Object... parameters) throws SQLException
	{
		if (getArrayMethodSet.contains(method))
		{
			return new ArrayProxyFactoryFactory<Z, D, ResultSet>(this.getProxyFactory().locatorsUpdateCopy());
		}
		if (getBlobMethodSet.contains(method))
		{
			return new BlobProxyFactoryFactory<Z, D, ResultSet>(this.getProxyFactory().locatorsUpdateCopy());
		}
		if (getClobMethodSet.contains(method))
		{
			return new ClobProxyFactoryFactory<Z, D, ResultSet, Clob>(Clob.class, this.getProxyFactory().locatorsUpdateCopy());
		}
		if (getNClobMethodSet.contains(method))
		{
			return new ClobProxyFactoryFactory<Z, D, ResultSet, NClob>(NClob.class, this.getProxyFactory().locatorsUpdateCopy());
		}
		if (getRefMethodSet.contains(method))
		{
			return new RefProxyFactoryFactory<Z, D, ResultSet>(this.getProxyFactory().locatorsUpdateCopy());
		}
		if (getSQLXMLMethodSet.contains(method))
		{
			return new SQLXMLProxyFactoryFactory<Z, D, ResultSet>(this.getProxyFactory().locatorsUpdateCopy());
		}
		
		return super.getProxyFactoryFactory(object, method, parameters);
	}

	@Override
	protected InvocationStrategy getInvocationStrategy(ResultSet resultSet, Method method, Object... parameters) throws SQLException
	{
		if (driverReadMethodSet.contains(method))
		{
			return InvocationStrategies.INVOKE_ON_ANY;
		}
		
		if (driverWriteMethodSet.contains(method) || method.equals(closeMethod))
		{
			return InvocationStrategies.INVOKE_ON_EXISTING;
		}
		
		if (transactionalWriteMethodSet.contains(method))
		{
			return this.getProxyFactory().getTransactionContext().start(InvocationStrategies.TRANSACTION_INVOKE_ON_ALL, this.getProxyFactory().getParentProxy().getConnection());
		}
		
		if (isGetMethod(method))
		{
			return InvocationStrategies.INVOKE_ON_ANY;
		}
		
		if (isUpdateMethod(method))
		{
			return InvocationStrategies.INVOKE_ON_EXISTING;
		}
		
		return super.getInvocationStrategy(resultSet, method, parameters);
	}

	@Override
	protected <R> Invoker<Z, D, ResultSet, R, SQLException> getInvoker(ResultSet results, final Method method, final Object... parameters) throws SQLException
	{
		if (isUpdateMethod(method) && (parameters.length > 1))
		{
			return this.getInvoker(method.getParameterTypes()[1], 1, results, method, parameters);
		}
		
		return super.getInvoker(results, method, parameters);
	}

	@Override
	protected <R> void postInvoke(Invoker<Z, D, ResultSet, R, SQLException> invoker, ResultSet results, Method method, Object... parameters)
	{
		if (method.equals(closeMethod))
		{
			this.getProxyFactory().remove();
		}
		
		if (driverWriteMethodSet.contains(method) || isUpdateMethod(method))
		{
			this.getProxyFactory().addInvoker(invoker);
		}
	}
	
	private static boolean isGetMethod(Method method)
	{
		Class<?>[] types = method.getParameterTypes();
		
		return method.getName().startsWith("get") && (types != null) && (types.length > 0) && (types[0].equals(String.class) || types[0].equals(Integer.TYPE));
	}
	
	private static boolean isUpdateMethod(Method method)
	{
		Class<?>[] types = method.getParameterTypes();
		
		return method.getName().startsWith("update") && (types != null) && (types.length > 0) && (types[0].equals(String.class) || types[0].equals(Integer.TYPE));
	}
}
