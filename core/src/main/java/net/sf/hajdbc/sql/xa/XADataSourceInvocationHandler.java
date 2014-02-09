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
package net.sf.hajdbc.sql.xa;

import java.lang.reflect.Method;
import java.sql.SQLException;
import java.util.Set;

import javax.sql.XADataSource;

import net.sf.hajdbc.invocation.InvocationStrategies;
import net.sf.hajdbc.invocation.InvocationStrategy;
import net.sf.hajdbc.sql.CommonDataSourceInvocationHandler;
import net.sf.hajdbc.sql.ProxyFactoryFactory;
import net.sf.hajdbc.util.reflect.Methods;

/**
 * @author Paul Ferraro
 *
 */
public class XADataSourceInvocationHandler extends CommonDataSourceInvocationHandler<XADataSource, XADataSourceDatabase, XADataSourceProxyFactory>
{
	private static final Set<Method> getXAConnectionMethodSet = Methods.findMethods(XADataSource.class, "getXAConnection");
	
	public XADataSourceInvocationHandler(XADataSourceProxyFactory factory)
	{
		super(XADataSource.class, factory);
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.sql.CommonDataSourceInvocationHandler#getInvocationStrategy(javax.sql.CommonDataSource, java.lang.reflect.Method, java.lang.Object[])
	 */
	@Override
	protected InvocationStrategy getInvocationStrategy(XADataSource dataSource, Method method, Object... parameters) throws SQLException
	{
		if (getXAConnectionMethodSet.contains(method))
		{
			return InvocationStrategies.TRANSACTION_INVOKE_ON_ALL;
		}
		return super.getInvocationStrategy(dataSource, method, parameters);
	}

	@Override
	protected ProxyFactoryFactory<XADataSource, XADataSourceDatabase, XADataSource, SQLException, ?, ? extends Exception> getProxyFactoryFactory(XADataSource object, Method method, Object... parameters) throws SQLException
	{
		if (getXAConnectionMethodSet.contains(method))
		{
			return new XAConnectionProxyFactoryFactory();
		}
		
		return super.getProxyFactoryFactory(object, method, parameters);
	}
}
