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

import javax.sql.XAConnection;
import javax.sql.XADataSource;

import net.sf.hajdbc.sql.ProxyFactoryFactory;
import net.sf.hajdbc.sql.pool.AbstractPooledConnectionInvocationHandler;
import net.sf.hajdbc.util.reflect.Methods;

/**
 * @author Paul Ferraro
 */
public class XAConnectionInvocationHandler extends AbstractPooledConnectionInvocationHandler<XADataSource, XADataSourceDatabase, XAConnection, XAConnectionProxyFactory>
{
	private static final Method getXAResource = Methods.getMethod(XAConnection.class, "getXAResource");
	
	public XAConnectionInvocationHandler(XAConnectionProxyFactory proxyFactory)
	{
		super(XAConnection.class, proxyFactory);
	}

	@Override
	protected ProxyFactoryFactory<XADataSource, XADataSourceDatabase, XAConnection, SQLException, ?, ? extends Exception> getProxyFactoryFactory(XAConnection object, Method method, Object... parameters) throws SQLException
	{
		if (method.equals(getXAResource))
		{
			return new XAResourceProxyFactoryFactory();
		}
		
		return super.getProxyFactoryFactory(object, method, parameters);
	}
}
