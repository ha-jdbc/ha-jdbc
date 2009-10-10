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

import javax.xml.bind.annotation.XmlEnum;
import javax.xml.bind.annotation.XmlEnumValue;

import net.sf.hajdbc.Database;
import net.sf.hajdbc.DatabaseCluster;
import net.sf.hajdbc.cache.eager.EagerDatabaseMetaDataCacheFactory;
import net.sf.hajdbc.cache.eager.SharedEagerDatabaseMetaDataCacheFactory;
import net.sf.hajdbc.cache.lazy.LazyDatabaseMetaDataCacheFactory;
import net.sf.hajdbc.cache.lazy.SharedLazyDatabaseMetaDataCacheFactory;
import net.sf.hajdbc.cache.simple.SimpleDatabaseMetaDataCacheFactory;

/**
 * Factory for creating DatabaseMetaDataCache implementations.
 * 
 * @author Paul Ferraro
 * @since 2.0
 */
@XmlEnum(String.class)
public enum DatabaseMetaDataCacheFactoryEnum implements DatabaseMetaDataCacheFactory
{
	@XmlEnumValue("none")
	NONE(new SimpleDatabaseMetaDataCacheFactory()),
	@XmlEnumValue("lazy")
	LAZY(new LazyDatabaseMetaDataCacheFactory()),
	@XmlEnumValue("eager")
	EAGER(new EagerDatabaseMetaDataCacheFactory()),
	@XmlEnumValue("shared-lazy")
	SHARED_LAZY(new SharedEagerDatabaseMetaDataCacheFactory()),
	@XmlEnumValue("shared-eager")
	SHARED_EAGER(new SharedLazyDatabaseMetaDataCacheFactory());
	
	private DatabaseMetaDataCacheFactory factory;
	
	private DatabaseMetaDataCacheFactoryEnum(DatabaseMetaDataCacheFactory factory)
	{
		this.factory = factory;
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.cache.DatabaseMetaDataCacheFactory#createCache(net.sf.hajdbc.DatabaseCluster)
	 */
	@Override
	public <Z, D extends Database<Z>> DatabaseMetaDataCache<Z, D> createCache(DatabaseCluster<Z, D> cluster)
	{
		return this.factory.createCache(cluster);
	}
}
