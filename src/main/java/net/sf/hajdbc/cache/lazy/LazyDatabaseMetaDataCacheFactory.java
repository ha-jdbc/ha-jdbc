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

import net.sf.hajdbc.Database;
import net.sf.hajdbc.DatabaseCluster;
import net.sf.hajdbc.cache.DatabaseMetaDataCache;
import net.sf.hajdbc.cache.DatabaseMetaDataCacheFactory;

/**
 * Factory for creating a {@link LazyDatabaseMetaDataCache}.
 * @author Paul Ferraro
 */
public class LazyDatabaseMetaDataCacheFactory implements DatabaseMetaDataCacheFactory
{
	private static final long serialVersionUID = -8343580190808280295L;

	@Override
	public String getId()
	{
		return "lazy";
	}

	@Override
	public <Z, D extends Database<Z>> DatabaseMetaDataCache<Z, D> createCache(DatabaseCluster<Z, D> cluster)
	{
		return new LazyDatabaseMetaDataCache<Z, D>(cluster);
	}
}
