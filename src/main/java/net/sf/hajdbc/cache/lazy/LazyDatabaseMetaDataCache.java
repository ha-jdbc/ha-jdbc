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
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.AbstractMap;
import java.util.Map;
import java.util.TreeMap;

import net.sf.hajdbc.Database;
import net.sf.hajdbc.DatabaseCluster;
import net.sf.hajdbc.DatabaseProperties;
import net.sf.hajdbc.cache.DatabaseMetaDataCache;
import net.sf.hajdbc.dialect.Dialect;
import net.sf.hajdbc.util.ref.ReferenceMap;
import net.sf.hajdbc.util.ref.SoftReferenceFactory;

/**
 * Per-database {@link DatabaseMetaDataCache} implementation that populates itself lazily.
 * @author Paul Ferraro
 */
public class LazyDatabaseMetaDataCache<Z, D extends Database<Z>> implements DatabaseMetaDataCache<Z, D>
{
	private final Map<D, Map.Entry<DatabaseProperties, LazyDatabaseMetaDataProvider>> map = new ReferenceMap<D, Map.Entry<DatabaseProperties, LazyDatabaseMetaDataProvider>>(new TreeMap<D, Reference<Map.Entry<DatabaseProperties, LazyDatabaseMetaDataProvider>>>(), SoftReferenceFactory.getInstance());
	private final DatabaseCluster<Z, D> cluster;

	public LazyDatabaseMetaDataCache(DatabaseCluster<Z, D> cluster)
	{
		this.cluster = cluster;
	}
	
	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.cache.DatabaseMetaDataCache#flush()
	 */
	@Override
	public void flush()
	{
		synchronized (this.map)
		{
			this.map.clear();
		}
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.cache.DatabaseMetaDataCache#getDatabaseProperties(net.sf.hajdbc.Database, java.sql.Connection)
	 */
	@Override
	public DatabaseProperties getDatabaseProperties(D database, Connection connection) throws SQLException
	{
		synchronized (this.map)
		{
			Map.Entry<DatabaseProperties, LazyDatabaseMetaDataProvider> entry = this.map.get(database);
			
			if (entry == null)
			{
				DatabaseMetaData metaData = connection.getMetaData();
				Dialect dialect = this.cluster.getDialect();
				LazyDatabaseMetaDataProvider provider = new LazyDatabaseMetaDataProvider(metaData);
				DatabaseProperties properties = new LazyDatabaseProperties(provider, dialect);
				
				entry = new AbstractMap.SimpleImmutableEntry<DatabaseProperties, LazyDatabaseMetaDataProvider>(properties, provider);

				this.map.put(database, entry);
			}
			else
			{
				entry.getValue().setConnection(connection);
			}
			
			return entry.getKey();
		}
	}
}
