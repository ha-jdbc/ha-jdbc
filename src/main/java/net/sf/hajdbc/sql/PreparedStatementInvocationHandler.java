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
import java.sql.PreparedStatement;
import java.util.Set;

import net.sf.hajdbc.Database;
import net.sf.hajdbc.util.reflect.Methods;

/**
 * @author Paul Ferraro
 * @param <D> 
 */
public class PreparedStatementInvocationHandler<Z, D extends Database<Z>> extends AbstractPreparedStatementInvocationHandler<Z, D, PreparedStatement, PreparedStatementProxyFactory<Z, D>>
{
	private static final Set<Method> setMethods = Methods.findMethods(PreparedStatement.class, "set\\w+");
	
	public PreparedStatementInvocationHandler(PreparedStatementProxyFactory<Z, D> proxyFactory)
	{
		super(PreparedStatement.class, proxyFactory, setMethods);
	}
}
