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
import java.sql.Blob;
import java.sql.SQLException;
import java.util.Set;

import net.sf.hajdbc.Database;
import net.sf.hajdbc.sql.io.OutputStreamProxyFactoryFactory;
import net.sf.hajdbc.util.reflect.Methods;

/**
 * @author Paul Ferraro
 * @param <D> 
 * @param <P> 
 */
@SuppressWarnings("nls")
public class BlobInvocationHandler<Z, D extends Database<Z>, P> extends LocatorInvocationHandler<Z, D, P, Blob, BlobProxyFactory<Z, D, P>>
{
	private static final Method SET_BINARY_STREAM_METHOD = Methods.getMethod(Blob.class, "setBinaryStream", Long.TYPE);
	private static final Set<Method> READ_METHODS = Methods.findMethods(Blob.class, "getBinaryStream", "getBytes", "length", "position");
	private static final Set<Method> WRITE_METHODS = Methods.findMethods(Blob.class, "setBinaryStream", "setBytes", "truncate");

	public BlobInvocationHandler(BlobProxyFactory<Z, D, P> proxyFactory)
	{
		super(Blob.class, proxyFactory, READ_METHODS, WRITE_METHODS);
	}

	@Override
	protected ProxyFactoryFactory<Z, D, Blob, SQLException, ?, ? extends Exception> getProxyFactoryFactory(Blob object, Method method, Object... parameters) throws SQLException
	{
		if (method.equals(SET_BINARY_STREAM_METHOD))
		{
			return new OutputStreamProxyFactoryFactory<>();
		}
		
		return super.getProxyFactoryFactory(object, method, parameters);
	}
}
