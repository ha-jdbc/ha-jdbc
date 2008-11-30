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

import java.sql.Blob;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import net.sf.hajdbc.Database;

/**
 * @author Paul Ferraro
 * @param <D> 
 * @param <P> 
 */
@SuppressWarnings("nls")
public class BlobInvocationHandler<D, P> extends LocatorInvocationHandler<D, P, Blob>
{
	private static final Set<String> DATABASE_READ_METHOD_SET = new HashSet<String>(Arrays.asList("getBinaryStream", "getBytes", "length", "position"));

	/**
	 * @param object
	 * @param proxy
	 * @param invoker
	 * @param objectMap
	 * @throws Exception
	 */
	protected BlobInvocationHandler(P object, SQLProxy<D, P> proxy, Invoker<D, P, Blob> invoker, Map<Database<D>, Blob> objectMap) throws Exception
	{
		super(object, proxy, invoker, Blob.class, objectMap);
	}
	
	/**
	 * @see net.sf.hajdbc.sql.LocatorInvocationHandler#getDatabaseReadMethodSet()
	 */
	@Override
	protected Set<String> getDatabaseReadMethodSet()
	{
		return DATABASE_READ_METHOD_SET;
	}
}
