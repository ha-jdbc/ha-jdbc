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
package net.sf.hajdbc.sync;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.AbstractMap;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import net.sf.hajdbc.Database;
import net.sf.hajdbc.DatabaseCluster;
import net.sf.hajdbc.DatabaseProperties;
import net.sf.hajdbc.balancer.Balancer;
import net.sf.hajdbc.cache.DatabaseMetaDataCache;
import net.sf.hajdbc.codec.Decoder;
import net.sf.hajdbc.dialect.Dialect;
import net.sf.hajdbc.logging.Level;
import net.sf.hajdbc.logging.Logger;
import net.sf.hajdbc.logging.LoggerFactory;

/**
 * @author Paul Ferraro
 * @param <D> Driver or DataSource
 */
public class SynchronizationContextImpl<Z, D extends Database<Z>> implements SynchronizationContext<Z, D>
{
	private static final Logger logger = LoggerFactory.getLogger(SynchronizationContextImpl.class);
	
	private final Set<D> activeDatabaseSet;
	private final D sourceDatabase;
	private final D targetDatabase;
	private final DatabaseCluster<Z, D> cluster;
	private final DatabaseProperties sourceDatabaseProperties;
	private final DatabaseProperties targetDatabaseProperties;
	private final Map<D, Map.Entry<Connection, Boolean>> connectionMap = new HashMap<>();
	private final ExecutorService executor;
	
	/**
	 * @param cluster
	 * @param database
	 * @throws SQLException
	 */
	public SynchronizationContextImpl(DatabaseCluster<Z, D> cluster, D database) throws SQLException
	{
		this.cluster = cluster;
		
		Balancer<Z, D> balancer = cluster.getBalancer();
		
		this.sourceDatabase = balancer.next();
		
		this.activeDatabaseSet = balancer;
		this.targetDatabase = database;
		this.executor = Executors.newFixedThreadPool(this.activeDatabaseSet.size(), this.cluster.getThreadFactory());
		
		DatabaseMetaDataCache<Z, D> cache = cluster.getDatabaseMetaDataCache();
		
		this.targetDatabaseProperties = cache.getDatabaseProperties(this.targetDatabase, this.getConnection(this.targetDatabase));
		this.sourceDatabaseProperties = cache.getDatabaseProperties(this.sourceDatabase, this.getConnection(this.sourceDatabase));
	}
	
	/**
	 * @see net.sf.hajdbc.sync.SynchronizationContext#getConnection(net.sf.hajdbc.Database)
	 */
	@Override
	public Connection getConnection(D database) throws SQLException
	{
		Map.Entry<Connection, Boolean> entry = this.connectionMap.get(database);
		
		if (entry == null)
		{
			Connection connection = database.connect(database.createConnectionSource(), database.decodePassword(this.cluster.getDecoder()));
			entry = new AbstractMap.SimpleImmutableEntry<>(connection, connection.getAutoCommit());
			
			this.connectionMap.put(database, entry);
		}
		
		return entry.getKey();
	}
	
	/**
	 * @see net.sf.hajdbc.sync.SynchronizationContext#getSourceDatabase()
	 */
	@Override
	public D getSourceDatabase()
	{
		return this.sourceDatabase;
	}
	
	/**
	 * @see net.sf.hajdbc.sync.SynchronizationContext#getTargetDatabase()
	 */
	@Override
	public D getTargetDatabase()
	{
		return this.targetDatabase;
	}
	
	/**
	 * @see net.sf.hajdbc.sync.SynchronizationContext#getActiveDatabaseSet()
	 */
	@Override
	public Set<D> getActiveDatabaseSet()
	{
		return this.activeDatabaseSet;
	}
	
	/**
	 * @see net.sf.hajdbc.sync.SynchronizationContext#getSourceDatabaseProperties()
	 */
	@Override
	public DatabaseProperties getSourceDatabaseProperties()
	{
		return this.sourceDatabaseProperties;
	}

	/**
	 * @see net.sf.hajdbc.sync.SynchronizationContext#getTargetDatabaseProperties()
	 */
	@Override
	public DatabaseProperties getTargetDatabaseProperties()
	{
		return this.targetDatabaseProperties;
	}

	/**
	 * @see net.sf.hajdbc.sync.SynchronizationContext#getDialect()
	 */
	@Override
	public Dialect getDialect()
	{
		return this.cluster.getDialect();
	}
	
	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.sync.SynchronizationContext#getDecoder()
	 */
	@Override
	public Decoder getDecoder()
	{
		return this.cluster.getDecoder();
	}

	/**
	 * @see net.sf.hajdbc.sync.SynchronizationContext#getExecutor()
	 */
	@Override
	public ExecutorService getExecutor()
	{
		return this.executor;
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.sync.SynchronizationContext#getSynchronizationSupport()
	 */
	@Override
	public SynchronizationSupport getSynchronizationSupport()
	{
		return new SynchronizationSupportImpl<>(this);
	}

	/**
	 * @see net.sf.hajdbc.sync.SynchronizationContext#close()
	 */
	@Override
	public void close()
	{
		for (Map.Entry<Connection, Boolean> entry: this.connectionMap.values())
		{
			try (Connection connection = entry.getKey())
			{
				connection.setAutoCommit(entry.getValue());
			}
			catch (SQLException e)
			{
				logger.log(Level.WARN, e);
			}
		}
		
		this.executor.shutdown();
	}
}
