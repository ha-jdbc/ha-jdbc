/*
 * HA-JDBC: High-Availability JDBC
 * Copyright (C) 2012  Paul Ferraro
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
package net.sf.hajdbc.cache.lazy;

import java.lang.ref.Reference;
import java.lang.ref.SoftReference;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.AbstractMap;
import java.util.Map;

import net.sf.hajdbc.Database;
import net.sf.hajdbc.DatabaseCluster;
import net.sf.hajdbc.DatabaseProperties;
import net.sf.hajdbc.cache.DatabaseMetaDataCache;
import net.sf.hajdbc.cache.DatabaseMetaDataSupport;
import net.sf.hajdbc.cache.DatabaseMetaDataSupportFactory;
import net.sf.hajdbc.dialect.Dialect;


/**
 * DatabaseMetaDataCache implementation that lazily caches data when requested.
 * Used when a compromise between memory usage and performance is desired.
 * Caches DatabaseProperties using a soft reference to prevent <code>OutOfMemoryError</code>s.
 * 
 * @author Paul Ferraro
 * @since 2.0
 */
public class SharedLazyDatabaseMetaDataCache<Z, D extends Database<Z>> implements DatabaseMetaDataCache<Z, D>
{
	private final DatabaseCluster<Z, D> cluster;
	private final DatabaseMetaDataSupportFactory factory;
	
	private volatile Reference<Map.Entry<DatabaseProperties, LazyDatabaseMetaDataProvider>> entryRef = new SoftReference<Map.Entry<DatabaseProperties, LazyDatabaseMetaDataProvider>>(null);
	
	public SharedLazyDatabaseMetaDataCache(DatabaseCluster<Z, D> cluster, DatabaseMetaDataSupportFactory factory)
	{
		this.cluster = cluster;
		this.factory = factory;
	}
	
	/**
	 * @see net.sf.hajdbc.cache.DatabaseMetaDataCache#flush()
	 */
	@Override
	public synchronized void flush()
	{
		this.entryRef.clear();
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.cache.DatabaseMetaDataCache#getDatabaseProperties(net.sf.hajdbc.Database, java.sql.Connection)
	 */
	@Override
	public DatabaseProperties getDatabaseProperties(D database, Connection connection) throws SQLException
	{
		Map.Entry<DatabaseProperties, LazyDatabaseMetaDataProvider> entry = this.entryRef.get();
		
		if (entry == null)
		{
			DatabaseMetaData metaData = connection.getMetaData();
			Dialect dialect = this.cluster.getDialect();
			DatabaseMetaDataSupport support = this.factory.createSupport(metaData, dialect);
			LazyDatabaseMetaDataProvider provider = new LazyDatabaseMetaDataProvider(metaData);
			DatabaseProperties properties = new LazyDatabaseProperties(provider, support, dialect);
			
			entry = new AbstractMap.SimpleImmutableEntry<DatabaseProperties, LazyDatabaseMetaDataProvider>(properties, provider);
		
			this.entryRef = new SoftReference<Map.Entry<DatabaseProperties, LazyDatabaseMetaDataProvider>>(entry);
		}
		else
		{
			entry.getValue().setConnection(connection);
		}
		
		return entry.getKey();
	}
}
