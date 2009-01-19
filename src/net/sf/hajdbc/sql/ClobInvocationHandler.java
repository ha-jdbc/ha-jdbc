/*
 * HA-JDBC: High-Availability JDBC
 * Copyright (c) 2004-2007 Paul Ferraro
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
import java.util.Map;
import java.util.Set;

import net.sf.hajdbc.Database;
import net.sf.hajdbc.util.reflect.Methods;

/**
 * @author Paul Ferraro
 * @param <D> 
 * @param <P> 
 */
@SuppressWarnings("nls")
public class ClobInvocationHandler<D, P> extends LocatorInvocationHandler<D, P, Clob>
{
	private static final Set<Method> DATABASE_READ_METHOD_SET = Methods.findMethods(Clob.class, "getAsciiStream", "getCharacterStream", "getSubString", "length", "position");
	
	/**
	 * @param object
	 * @param proxy
	 * @param invoker
	 * @param objectMap
	 * @throws Exception
	 */
	protected ClobInvocationHandler(P object, SQLProxy<D, P> proxy, Invoker<D, P, Clob> invoker, Map<Database<D>, Clob> objectMap) throws Exception
	{
		super(object, proxy, invoker, Clob.class, objectMap);
	}

	/**
	 * @see net.sf.hajdbc.sql.LocatorInvocationHandler#getDatabaseReadMethodSet()
	 */
	@Override
	protected Set<Method> getDatabaseReadMethodSet()
	{
		return DATABASE_READ_METHOD_SET;
	}
}
