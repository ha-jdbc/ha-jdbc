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

import net.sf.hajdbc.DatabaseMetaDataCacheFactory;
import net.sf.hajdbc.Messages;
import net.sf.hajdbc.util.ClassEnum;
import net.sf.hajdbc.util.Enums;

/**
 * Factory for creating DatabaseMetaDataCache implementations.
 * 
 * @author Paul Ferraro
 * @since 2.0
 */
public enum DatabaseMetaDataCacheFactoryClass implements ClassEnum<DatabaseMetaDataCacheFactory>
{
	NONE(SimpleDatabaseMetaDataCacheFactory.class),
	LAZY(LazyDatabaseMetaDataCacheFactory.class),
	EAGER(EagerDatabaseMetaDataCacheFactory.class);
	
	private Class<? extends DatabaseMetaDataCacheFactory> cacheFactoryClass;
	
	private DatabaseMetaDataCacheFactoryClass(Class<? extends DatabaseMetaDataCacheFactory> cacheFactoryClass)
	{
		this.cacheFactoryClass = cacheFactoryClass;
	}
	
	/**
	 * @see net.sf.hajdbc.util.ClassEnum#isInstance(java.lang.Object)
	 */
	@Override
	public boolean isInstance(DatabaseMetaDataCacheFactory cache)
	{
		return this.cacheFactoryClass.equals(cache.getClass());
	}
	
	/**
	 * @see net.sf.hajdbc.util.ClassEnum#newInstance()
	 */
	@Override
	public DatabaseMetaDataCacheFactory newInstance() throws Exception
	{
		return this.cacheFactoryClass.newInstance();
	}
	
	/**
	 * Creates a new instance of the {@link net.sf.hajdbc.DatabaseMetaDataCacheFactory} implementation identified by the specified identifier
	 * @param id an enumerated cache identifier
	 * @return a new cache factory instance
	 * @throws Exception if specified cache identifier is invalid
	 */
	public static DatabaseMetaDataCacheFactory deserialize(String id) throws Exception
	{
		try
		{
			return Enums.valueOf(DatabaseMetaDataCacheFactoryClass.class, id).newInstance();
		}
		catch (IllegalArgumentException e)
		{
			throw new IllegalArgumentException(Messages.getMessage(Messages.INVALID_META_DATA_CACHE, id));
		}
	}
	
	/**
	 * Return the identifier of the specified {@link net.sf.hajdbc.DatabaseMetaDataCacheFactory}.
	 * @param factory a cache factory implementation
	 * @return the class name of this cache
	 */
	public static String serialize(DatabaseMetaDataCacheFactory factory)
	{
		for (DatabaseMetaDataCacheFactoryClass cacheFactoryClass: DatabaseMetaDataCacheFactoryClass.values())
		{
			if (cacheFactoryClass.isInstance(factory))
			{
				return Enums.id(cacheFactoryClass);
			}
		}
		
		throw new IllegalArgumentException(Messages.getMessage(Messages.INVALID_META_DATA_CACHE, factory.getClass()));
	}
}
