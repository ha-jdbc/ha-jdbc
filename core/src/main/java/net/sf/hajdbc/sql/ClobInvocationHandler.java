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
import java.sql.SQLException;
import java.util.Set;

import net.sf.hajdbc.Database;
import net.sf.hajdbc.sql.io.OutputStreamProxyFactoryFactory;
import net.sf.hajdbc.sql.io.WriterProxyFactoryFactory;
import net.sf.hajdbc.util.reflect.Methods;

/**
 * @author paul
 *
 */
public class ClobInvocationHandler<Z, D extends Database<Z>, P, C extends Clob> extends LocatorInvocationHandler<Z, D, P, C, ClobProxyFactory<Z, D, P, C>>
{
	private static final Method SET_ASCII_STREAM_METHOD = Methods.getMethod(Clob.class, "setAsciiStream", Long.TYPE);
	private static final Method SET_CHARACTER_STREAM_METHOD = Methods.getMethod(Clob.class, "setCharacterStream", Long.TYPE);
	private static final Set<Method> READ_METHODS = Methods.findMethods(Clob.class, "getAsciiStream", "getCharacterStream", "getSubString", "length", "position");
	private static final Set<Method> WRITE_METHODS = Methods.findMethods(Clob.class, "setAsciiStream", "setCharacterStream", "setString", "truncate");
	
	public ClobInvocationHandler(Class<C> proxyClass, ClobProxyFactory<Z, D, P, C> proxyFactory)
	{
		super(proxyClass, proxyFactory, READ_METHODS, WRITE_METHODS);
	}

	@Override
	protected ProxyFactoryFactory<Z, D, C, SQLException, ?, ? extends Exception> getProxyFactoryFactory(C object, Method method, Object... parameters) throws SQLException
	{
		if (method.equals(SET_ASCII_STREAM_METHOD))
		{
			return new OutputStreamProxyFactoryFactory<>();
		}
		if (method.equals(SET_CHARACTER_STREAM_METHOD))
		{
			return new WriterProxyFactoryFactory<>();
		}
		
		return super.getProxyFactoryFactory(object, method, parameters);
	}
}
