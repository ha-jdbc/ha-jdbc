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

import java.sql.SQLException;
import java.util.Map;

import javax.sql.XAConnection;
import javax.sql.XADataSource;
import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;

import net.sf.hajdbc.invocation.Invoker;
import net.sf.hajdbc.sql.ProxyFactory;
import net.sf.hajdbc.sql.ProxyFactoryFactory;

/**
 * @author Paul Ferraro
 */
public class XAResourceProxyFactoryFactory implements ProxyFactoryFactory<XADataSource, XADataSourceDatabase, XAConnection, SQLException, XAResource, XAException>
{
	@Override
	public ProxyFactory<XADataSource, XADataSourceDatabase, XAResource, XAException> createProxyFactory(XAConnection parentProxy, ProxyFactory<XADataSource, XADataSourceDatabase, XAConnection, SQLException> parent, Invoker<XADataSource, XADataSourceDatabase, XAConnection, XAResource, SQLException> invoker, Map<XADataSourceDatabase, XAResource> resources)
	{
		return new XAResourceProxyFactory(parentProxy, parent, invoker, resources);
	}
}
