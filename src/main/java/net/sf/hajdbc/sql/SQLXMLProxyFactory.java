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
package net.sf.hajdbc.sql;

import java.sql.SQLException;
import java.sql.SQLXML;
import java.util.Map;

import net.sf.hajdbc.Database;
import net.sf.hajdbc.invocation.Invoker;
import net.sf.hajdbc.util.reflect.Proxies;

/**
 * 
 * @author Paul Ferraro
 */
public class SQLXMLProxyFactory<Z, D extends Database<Z>, P> extends LocatorProxyFactory<Z, D, P, SQLXML>
{
	public SQLXMLProxyFactory(P parentProxy, ProxyFactory<Z, D, P, SQLException> parent, Invoker<Z, D, P, SQLXML, SQLException> invoker, Map<D, SQLXML> locators, boolean locatorsUpdateCopy)
	{
		super(parentProxy, parent, invoker, locators, locatorsUpdateCopy);
	}

	@Override
	public void close(D database, SQLXML xml) throws SQLException
	{
		xml.free();
	}

	@Override
	public SQLXML createProxy()
	{
		return Proxies.createProxy(SQLXML.class, new SQLXMLInvocationHandler<>(this));
	}
}
