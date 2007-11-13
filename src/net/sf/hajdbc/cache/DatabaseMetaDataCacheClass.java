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
package net.sf.hajdbc.cache;

import net.sf.hajdbc.DatabaseMetaDataCache;
import net.sf.hajdbc.Messages;
import net.sf.hajdbc.util.ClassEnum;

/**
 * Factory for creating DatabaseMetaDataCache implementations.
 * 
 * @author Paul Ferraro
 * @since 2.0
 */
public enum DatabaseMetaDataCacheClass implements ClassEnum<DatabaseMetaDataCache>
{
	NONE(NullDatabaseMetaDataCache.class),
	LAZY(LazyDatabaseMetaDataCache.class),
	EAGER(EagerDatabaseMetaDataCache.class);
	
	private Class<? extends DatabaseMetaDataCache> cacheClass;
	
	private DatabaseMetaDataCacheClass(Class<? extends DatabaseMetaDataCache> cacheClass)
	{
		this.cacheClass = cacheClass;
	}
	
	/**
	 * @see net.sf.hajdbc.util.ClassEnum#isInstance(java.lang.Object)
	 */
	@Override
	public boolean isInstance(DatabaseMetaDataCache cache)
	{
		return this.cacheClass.equals(cache.getClass());
	}
	
	/**
	 * @see net.sf.hajdbc.util.ClassEnum#newInstance()
	 */
	@Override
	public DatabaseMetaDataCache newInstance() throws Exception
	{
		return this.cacheClass.newInstance();
	}
	
	/**
	 * Creates a new instance of the DatabaseMetaDataCache implementation identified by the specified identifier
	 * @param id an enumerated cache identifier
	 * @return a new DatabaseMetaDataCache instance
	 * @throws Exception if specified cache identifier is invalid
	 */
	public static DatabaseMetaDataCache deserialize(String id) throws Exception
	{
		try
		{
			return DatabaseMetaDataCacheClass.valueOf(id.toUpperCase()).newInstance();
		}
		catch (IllegalArgumentException e)
		{
			throw new IllegalArgumentException(Messages.getMessage(Messages.INVALID_META_DATA_CACHE, id));
		}
	}
	
	/**
	 * Return the identifier of the specified DatabaseMetaDataCache.
	 * @param cache a cache implementation
	 * @return the class name of this cache
	 */
	public static String serialize(DatabaseMetaDataCache cache)
	{
		for (DatabaseMetaDataCacheClass cacheClass: DatabaseMetaDataCacheClass.values())
		{
			if (cacheClass.isInstance(cache))
			{
				return cacheClass.name().toLowerCase();
			}
		}
		
		throw new IllegalArgumentException(Messages.getMessage(Messages.INVALID_META_DATA_CACHE, cache.getClass()));
	}
}
