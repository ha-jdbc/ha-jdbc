/*
 * HA-JDBC: High-Availability JDBC
 * Copyright 2004-2009 Paul Ferraro
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
package net.sf.hajdbc.cache;

import javax.xml.bind.annotation.XmlEnum;
import javax.xml.bind.annotation.XmlEnumValue;
import javax.xml.bind.annotation.XmlType;

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
@XmlType(name = "databaseMetaDataCache")
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
