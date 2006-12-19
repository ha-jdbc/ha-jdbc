/*
 * HA-JDBC: High-Availability JDBC
 * Copyright (c) 2004-2006 Paul Ferraro
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
import net.sf.hajdbc.DatabaseMetaDataCache;
import net.sf.hajdbc.Dialect;
import net.sf.hajdbc.SynchronizationContext;
import net.sf.hajdbc.util.concurrent.DaemonThreadFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * @author Paul Ferraro
 * @since 1.2
 */
public class SynchronizationContextImpl implements SynchronizationContext
{
	private static Logger logger = LoggerFactory.getLogger(SynchronizationContextImpl.class);
	
	private Set<Database> activeDatabaseSet;
	private Database sourceDatabase;
	private Database targetDatabase;
	private DatabaseCluster cluster;
	private Map<Database, Connection> connectionMap = new HashMap<Database, Connection>();
	private ExecutorService executor;
	
	public SynchronizationContextImpl(DatabaseCluster cluster, Database database)
	{
		this.cluster = cluster;
		
		Balancer balancer = cluster.getBalancer();
		
		this.sourceDatabase = balancer.next();
		this.activeDatabaseSet = balancer.all();
		this.targetDatabase = database;
		this.executor = Executors.newFixedThreadPool(this.activeDatabaseSet.size(), DaemonThreadFactory.getInstance());
	}
	
	/**
	 * @see net.sf.hajdbc.SynchronizationContext#getConnection(net.sf.hajdbc.Database)
	 */
	@SuppressWarnings("unchecked")
	public Connection getConnection(Database database) throws SQLException
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
	public Database getSourceDatabase()
	{
		return this.sourceDatabase;
	}
	
	/**
	 * @see net.sf.hajdbc.SynchronizationContext#getTargetDatabase()
	 */
	public Database getTargetDatabase()
	{
		return this.targetDatabase;
	}
	
	/**
	 * @see net.sf.hajdbc.SynchronizationContext#getActiveDatabases()
	 */
	public Set<Database> getActiveDatabaseSet()
	{
		return this.activeDatabaseSet;
	}
	
	/**
	 * @see net.sf.hajdbc.SynchronizationContext#getDatabaseMetaDataCache()
	 */
	public DatabaseMetaDataCache getDatabaseMetaDataCache()
	{
		return this.cluster.getDatabaseMetaDataCache();
	}
	
	/**
	 * @see net.sf.hajdbc.SynchronizationContext#getDialect()
	 */
	public Dialect getDialect()
	{
		return this.cluster.getDialect();
	}
	
	/**
	 * @see net.sf.hajdbc.SynchronizationContext#getExecutor()
	 */
	public ExecutorService getExecutor()
	{
		return this.executor;
	}

	/**
	 * @see net.sf.hajdbc.SynchronizationContext#close()
	 */
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
				catch (java.sql.SQLException e)
				{
					logger.warn(e.toString(), e);
				}
			}
		}
		
		this.executor.shutdown();
	}
}