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
package net.sf.hajdbc.cache.eager;

import java.sql.Connection;
import java.sql.SQLException;

import net.sf.hajdbc.Database;
import net.sf.hajdbc.DatabaseCluster;
import net.sf.hajdbc.Messages;
import net.sf.hajdbc.cache.DatabaseMetaDataCache;
import net.sf.hajdbc.cache.DatabaseMetaDataSupportFactory;
import net.sf.hajdbc.cache.DatabaseProperties;

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
	 * @see net.sf.hajdbc.cache.DatabaseMetaDataCache#flush()
	 */
	@Override
	public void flush() throws SQLException
	{
		D database = this.cluster.getBalancer().next();
		
		if (database == null)
		{
			throw new SQLException(Messages.NO_ACTIVE_DATABASES.getMessage());
		}
		
		this.setDatabaseProperties(database.connect(database.createConnectionSource()));
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.cache.DatabaseMetaDataCache#getDatabaseProperties(net.sf.hajdbc.Database, java.sql.Connection)
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
		this.properties = new EagerDatabaseProperties(connection.getMetaData(), this.factory, this.cluster.getDialect());
	}
}