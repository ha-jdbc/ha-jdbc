/*
 * HA-JDBC: High-Availability JDBC
 * Copyright (C) 2013  Paul Ferraro
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

import java.sql.SQLException;
import java.util.Map;

import net.sf.hajdbc.invocation.Invoker;
import net.sf.hajdbc.sql.ProxyFactory;
import net.sf.hajdbc.sql.pool.AbstractPooledConnectionProxyFactory;
import net.sf.hajdbc.util.reflect.Proxies;

import javax.sql.XAConnection;
import javax.sql.XADataSource;

/**
 * 
 * @author Paul Ferraro
 */
public class XAConnectionProxyFactory extends AbstractPooledConnectionProxyFactory<XADataSource, XADataSourceDatabase, XAConnection>
{
	public XAConnectionProxyFactory(XADataSource parentProxy, ProxyFactory<XADataSource, XADataSourceDatabase, XADataSource, SQLException> parent, Invoker<XADataSource, XADataSourceDatabase, XADataSource, XAConnection, SQLException> invoker, Map<XADataSourceDatabase, XAConnection> map)
	{
		super(parentProxy, parent, invoker, map);
	}

	@Override
	public XAConnection createProxy()
	{
		return Proxies.createProxy(XAConnection.class, new XAConnectionInvocationHandler(this));
	}
}
