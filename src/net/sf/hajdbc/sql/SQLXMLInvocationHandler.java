/*
 * HA-JDBC: High-Availability JDBC
 * Copyright (c) 2004-2008 Paul Ferraro
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
import java.util.Map;
import java.util.Set;

import net.sf.hajdbc.Database;
import net.sf.hajdbc.util.reflect.Methods;

public class SQLXMLInvocationHandler<D, P> extends LocatorInvocationHandler<D, P, java.sql.SQLXML>
{
	private static final Set<Method> DATABASE_READ_METHOD_SET = Methods.findMethods(java.sql.SQLXML.class, "getBinaryStream", "getCharacterStream", "getSource", "getString");

	protected SQLXMLInvocationHandler(P object, SQLProxy<D, P> proxy, Invoker<D, P, java.sql.SQLXML> invoker, Map<Database<D>, java.sql.SQLXML> objectMap) throws Exception
	{
		super(object, proxy, invoker, java.sql.SQLXML.class, objectMap);
	}

	@Override
	protected Set<Method> getDatabaseReadMethodSet()
	{
		return DATABASE_READ_METHOD_SET;
	}
}