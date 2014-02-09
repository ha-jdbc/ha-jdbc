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

import java.sql.Ref;
import java.sql.SQLException;
import java.util.Map;

import net.sf.hajdbc.Database;
import net.sf.hajdbc.invocation.Invoker;

/**
 * 
 * @author Paul Ferraro
 */
public class RefProxyFactoryFactory<Z, D extends Database<Z>, P> implements ProxyFactoryFactory<Z, D, P, SQLException, Ref, SQLException>
{
	private final boolean locatorsUpdateCopy;
	
	public RefProxyFactoryFactory(boolean locatorsUpdateCopy)
	{
		this.locatorsUpdateCopy = locatorsUpdateCopy;
	}

	@Override
	public ProxyFactory<Z, D, Ref, SQLException> createProxyFactory(P parentProxy, ProxyFactory<Z, D, P, SQLException> parent, Invoker<Z, D, P, Ref, SQLException> invoker, Map<D, Ref> structs)
	{
		return new RefProxyFactory<>(parentProxy, parent, invoker, structs, this.locatorsUpdateCopy);
	}
}
