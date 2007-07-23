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
package net.sf.hajdbc.sync;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import net.sf.hajdbc.Balancer;
import net.sf.hajdbc.Database;
import net.sf.hajdbc.DatabaseCluster;
import net.sf.hajdbc.DatabaseProperties;
import net.sf.hajdbc.Dialect;
import net.sf.hajdbc.SynchronizationContext;
import net.sf.hajdbc.util.concurrent.DaemonThreadFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * @author Paul Ferraro
 * @since 2.0
 */
public class SynchronizationContextImpl<D> implements SynchronizationContext<D>
{
	private static Logger logger = LoggerFactory.getLogger(SynchronizationContextImpl.class);
	
	private Set<Database<D>> activeDatabaseSet;
	private Database<D> sourceDatabase;
	private Database<D> targetDatabase;
	private DatabaseCluster<D> cluster;
	private DatabaseProperties databaseProperties;
	private Map<Database<D>, Connection> connectionMap = new HashMap<Database<D>, Connection>();
	private ExecutorService executor;
	
	public SynchronizationContextImpl(DatabaseCluster<D> cluster, Database<D> database) throws SQLException
	{
		this.cluster = cluster;
		
		Balancer<D> balancer = cluster.getBalancer();
		
		this.sourceDatabase = balancer.next();
		this.activeDatabaseSet = balancer.all();
		this.targetDatabase = database;
		this.executor = Executors.newFixedThreadPool(this.activeDatabaseSet.size(), DaemonThreadFactory.getInstance());
		this.databaseProperties = cluster.getDatabaseMetaDataCache().getDatabaseProperties(this.getConnection(this.targetDatabase));
	}
	
	/**
	 * @see net.sf.hajdbc.SynchronizationContext#getConnection(net.sf.hajdbc.Database)
	 */
	@Override
	public Connection getConnection(Database<D> database) throws SQLException
	{
		synchronized (this.connectionMap)
		{
			Connection connection = this.connectionMap.get(database);
			
			if (connection == null)
			{
				connection = database.connect(this.cluster.getConnectionFactoryMap().get(database));
				
				this.connectionMap.put(database, connection);
			}
			
			return connection;
		}
	}
	
	/**
	 * @see net.sf.hajdbc.SynchronizationContext#getSourceDatabase()
	 */
	@Override
	public Database<D> getSourceDatabase()
	{
		return this.sourceDatabase;
	}
	
	/**
	 * @see net.sf.hajdbc.SynchronizationContext#getTargetDatabase()
	 */
	@Override
	public Database<D> getTargetDatabase()
	{
		return this.targetDatabase;
	}
	
	/**
	 * @see net.sf.hajdbc.SynchronizationContext#getActiveDatabaseSet()
	 */
	@Override
	public Set<Database<D>> getActiveDatabaseSet()
	{
		return this.activeDatabaseSet;
	}
	
	/**
	 * @see net.sf.hajdbc.SynchronizationContext#getDatabaseProperties()
	 */
	@Override
	public DatabaseProperties getDatabaseProperties()
	{
		return this.databaseProperties;
	}
	
	/**
	 * @see net.sf.hajdbc.SynchronizationContext#getDialect()
	 */
	@Override
	public Dialect getDialect()
	{
		return this.cluster.getDialect();
	}
	
	/**
	 * @see net.sf.hajdbc.SynchronizationContext#getExecutor()
	 */
	@Override
	public ExecutorService getExecutor()
	{
		return this.executor;
	}

	/**
	 * @see net.sf.hajdbc.SynchronizationContext#close()
	 */
	@Override
	public void close()
	{
		for (Connection connection: this.connectionMap.values())
		{
			if (connection != null)
			{
				try
				{
					if (!connection.isClosed())
					{
						connection.close();
					}
				}
				catch (SQLException e)
				{
					logger.warn(e.toString(), e);
				}
			}
		}
		
		this.executor.shutdown();
	}
}