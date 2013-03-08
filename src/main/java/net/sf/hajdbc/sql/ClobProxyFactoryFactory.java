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

import java.sql.Clob;
import java.sql.SQLException;
import java.util.Map;

import net.sf.hajdbc.Database;
import net.sf.hajdbc.invocation.Invoker;

/**
 * @author paul
 *
 */
public class ClobProxyFactoryFactory<Z, D extends Database<Z>, P, C extends Clob> implements ProxyFactoryFactory<Z, D, P, SQLException, C, SQLException>
{
	private final Class<C> clobClass;
	private final boolean locatorsUpdateCopy;
	
	public ClobProxyFactoryFactory(Class<C> clobClass, boolean locatorsUpdateCopy)
	{
		this.clobClass = clobClass;
		this.locatorsUpdateCopy = locatorsUpdateCopy;
	}
	
	@Override
	public ProxyFactory<Z, D, C, SQLException> createProxyFactory(P parentProxy, ProxyFactory<Z, D, P, SQLException> parent, Invoker<Z, D, P, C, SQLException> invoker, Map<D, C> clobs)
	{
		return new ClobProxyFactory<Z, D, P, C>(this.clobClass, parentProxy, parent, invoker, clobs, this.locatorsUpdateCopy);
	}
}
