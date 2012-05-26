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
package net.sf.hajdbc.cache.eager;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;

import net.sf.hajdbc.Database;
import net.sf.hajdbc.DatabaseCluster;
import net.sf.hajdbc.DatabaseMetaDataCache;
import net.sf.hajdbc.DatabaseProperties;
import net.sf.hajdbc.Dialect;
import net.sf.hajdbc.Messages;
import net.sf.hajdbc.cache.DatabaseMetaDataSupport;
import net.sf.hajdbc.cache.DatabaseMetaDataSupportFactory;

/**
 * DatabaseMetaDataCache implementation that eagerly caches data when first flushed.
 * To be used when performance more of a concern than memory usage.
 * 
 * @author Paul Ferraro
 * @since 2.0
 */
public class SharedEagerDatabaseMetaDataCache<Z, D extends Database<Z>> implements DatabaseMetaDataCache<Z, D>
{
	private final DatabaseCluster<Z, D> cluster;
	private final DatabaseMetaDataSupportFactory factory;

	private volatile DatabaseProperties properties;
	
	public SharedEagerDatabaseMetaDataCache(DatabaseCluster<Z, D> cluster, DatabaseMetaDataSupportFactory factory)
	{
		this.cluster = cluster;
		this.factory = factory;
	}
	
	/**
	 * @see net.sf.hajdbc.DatabaseMetaDataCache#flush()
	 */
	@Override
	public void flush() throws SQLException
	{
		D database = this.cluster.getBalancer().next();
		
		if (database == null)
		{
			throw new SQLException(Messages.NO_ACTIVE_DATABASES.getMessage());
		}
		
		this.setDatabaseProperties(database.connect(database.createConnectionSource(), database.decodePassword(this.cluster.getCodec())));
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.DatabaseMetaDataCache#getDatabaseProperties(net.sf.hajdbc.Database, java.sql.Connection)
	 */
	@Override
	public synchronized DatabaseProperties getDatabaseProperties(D database, Connection connection) throws SQLException
	{
		if (this.properties == null)
		{
			this.setDatabaseProperties(connection);
		}
		
		return this.properties;
	}
	
	private synchronized void setDatabaseProperties(Connection connection) throws SQLException
	{
		DatabaseMetaData metaData = connection.getMetaData();
		Dialect dialect = this.cluster.getDialect();
		DatabaseMetaDataSupport support = this.factory.createSupport(metaData, dialect);
		this.properties = new EagerDatabaseProperties(metaData, support, dialect);
	}
}
