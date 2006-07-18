/*
 * HA-JDBC: High-Availability JDBC
 * Copyright (c) 2004-2006 Paul Ferraro
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
package net.sf.hajdbc.cache;

import java.util.HashMap;
import java.util.Map;

import net.sf.hajdbc.DatabaseMetaDataCache;
import net.sf.hajdbc.Messages;

/**
 * Factory for creating DatabaseMetaDataCache implementations.
 * 
 * @author Paul Ferraro
 * @since 1.2
 */
public class DatabaseMetaDataCacheFactory
{
	private static Map<String, Class<? extends DatabaseMetaDataCache>> cacheMap = new HashMap<String, Class<? extends DatabaseMetaDataCache>>();
	
	static
	{
		cacheMap.put("none", ThreadLocalDatabaseMetaDataCache.class);
		cacheMap.put("lazy", LazyDatabaseMetaDataCache.class);
		cacheMap.put("eager", EagerDatabaseMetaDataCache.class);
	}
	
	/**
	 * Creates a new instance of the DatabaseMetaDataCache implementation indentified by the specified identifier
	 * @param id an enumerated cache identifier
	 * @return a new DatabaseMetaDataCache instance
	 * @throws Exception if specified cache identifier is invalid
	 */
	public static DatabaseMetaDataCache deserialize(String id) throws Exception
	{
		Class<? extends DatabaseMetaDataCache> cacheClass = cacheMap.get(id);
		
		if (cacheClass == null)
		{
			throw new IllegalArgumentException(Messages.getMessage(Messages.INVALID_META_DATA_CACHE, id));
		}
		
		return cacheClass.newInstance();
	}
	
	/**
	 * Return the identifier of the specified DatabaseMetaDataCache.
	 * @param cache a cache implementation
	 * @return the class name of this cache
	 */
	public static String serialize(DatabaseMetaDataCache cache)
	{
		Class<? extends DatabaseMetaDataCache> cacheClass = cache.getClass();
		
		for (Map.Entry<String, Class<? extends DatabaseMetaDataCache>> cacheMapEntry: cacheMap.entrySet())
		{
			if (cacheClass.equals(cacheMapEntry.getValue()))
			{
				return cacheMapEntry.getKey();
			}
		}
		
		throw new IllegalArgumentException(Messages.getMessage(Messages.INVALID_META_DATA_CACHE, cacheClass));
	}
	
	private DatabaseMetaDataCacheFactory()
	{
		// Hide constructor
	}
}
