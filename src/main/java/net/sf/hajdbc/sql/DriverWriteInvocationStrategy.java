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

import java.util.Collections;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import net.sf.hajdbc.Database;

/**
 * @author Paul Ferraro
 *
 */
public class DriverWriteInvocationStrategy<Z, D extends Database<Z>, T, R, E extends Exception> extends AbstractInvocationStrategy<Z, D, T, R, E>
{
	@Override
	protected Map.Entry<SortedMap<D, R>, SortedMap<D, E>> collectResults(SQLProxy<Z, D, T, E> proxy, Invoker<Z, D, T, R, E> invoker)
	{
		final SortedMap<D, R> resultMap = new TreeMap<D, R>();
		final SortedMap<D, E> exceptionMap = new TreeMap<D, E>();

		for (Map.Entry<D, T> entry: proxy.entries())
		{
			D database = entry.getKey();
			
			try
			{
				resultMap.put(database, invoker.invoke(database, entry.getValue()));
			}
			catch (Exception e)
			{
				exceptionMap.put(database, proxy.getExceptionFactory().createException(e));
			}
		}
		
		return Collections.singletonMap(resultMap, exceptionMap).entrySet().iterator().next();
	}
}
