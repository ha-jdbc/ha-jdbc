/*
 * HA-JDBC: High-Availability JDBC
 * Copyright (c) 2004-2009 Paul Ferraro
 * 
 * This library is free software; you can redistribute it and/or modify it 
 * under the terms of the GNU Lesser General Public License as published by the 
 * Free Software Foundation; either version 2.1 of the License, or (at your 
 * option) any later version.
 * 
 * This library is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or 
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public License 
 * for more details.
 * 
 * You should have received a copy of the GNU Lesser General Public License
 * along with this library; if not, write to the Free Software Foundation, 
 * Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA
 * 
 * Contact: ferraro@users.sourceforge.net
 */
package net.sf.hajdbc.sql;

import java.lang.reflect.Method;
import java.sql.Clob;
import java.sql.SQLException;
import java.util.Map;
import java.util.Set;

import net.sf.hajdbc.Database;
import net.sf.hajdbc.util.reflect.Methods;

/**
 * @author paul
 *
 */
public abstract class AbstractClobInvocationHandler<Z, D extends Database<Z>, P, C extends Clob> extends LocatorInvocationHandler<Z, D, P, C>
{
	private static final Set<Method> READ_METHOD_SET = Methods.findMethods(Clob.class, "getAsciiStream", "getCharacterStream", "getSubString", "length", "position");
	private static final Set<Method> WRITE_METHOD_SET = Methods.findMethods(Clob.class, "setAsciiStream", "setCharacterStream", "setString", "truncate");
	
	/**
	 * @param object
	 * @param proxy
	 * @param invoker
	 * @param objectMap
	 * @throws Exception
	 */
	protected AbstractClobInvocationHandler(P object, SQLProxy<Z, D, P, SQLException> proxy, Invoker<Z, D, P, C, SQLException> invoker, Class<C> clobClass, Map<D, C> objectMap, boolean updateCopy)
	{
		super(object, proxy, invoker, clobClass, objectMap, updateCopy, READ_METHOD_SET, WRITE_METHOD_SET);
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.sql.LocatorInvocationHandler#free(java.lang.Object)
	 */
	@Override
	protected void free(C clob) throws SQLException
	{
		clob.free();
	}
}
