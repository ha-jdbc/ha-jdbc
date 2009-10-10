/*
 * HA-JDBC: High-Availability JDBC
 * Copyright (c) 2004-2009 Paul Ferraro
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
package net.sf.hajdbc.cache.lazy;

import java.lang.ref.Reference;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;
import java.util.TreeMap;

import net.sf.hajdbc.Database;
import net.sf.hajdbc.DatabaseCluster;
import net.sf.hajdbc.cache.DatabaseMetaDataCache;
import net.sf.hajdbc.cache.DatabaseMetaDataSupportFactory;
import net.sf.hajdbc.cache.DatabaseProperties;
import net.sf.hajdbc.util.ref.ReferenceMap;
import net.sf.hajdbc.util.ref.SoftReferenceFactory;

/**
 * @author paul
 *
 */
public class LazyDatabaseMetaDataCache<Z, D extends Database<Z>> implements DatabaseMetaDataCache<Z, D>
{
	private final Map<D, LazyDatabaseProperties> map = new ReferenceMap<D, LazyDatabaseProperties>(new TreeMap<D, Reference<LazyDatabaseProperties>>(), SoftReferenceFactory.getInstance());
	private final DatabaseCluster<Z, D> cluster;
	private final DatabaseMetaDataSupportFactory factory;

	public LazyDatabaseMetaDataCache(DatabaseCluster<Z, D> cluster, DatabaseMetaDataSupportFactory factory)
	{
		this.cluster = cluster;
		this.factory = factory;
	}
	
	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.cache.DatabaseMetaDataCache#flush()
	 */
	@Override
	public void flush() throws SQLException
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
			LazyDatabaseProperties properties = this.map.get(database);
			
			if (properties == null)
			{
				properties = new LazyDatabaseProperties(connection.getMetaData(), this.factory, this.cluster.getDialect());

				this.map.put(database, properties);
			}
			else
			{
				properties.setConnection(connection);
			}
			
			return properties;
		}
	}
}
