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
package net.sf.hajdbc;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * @author Paul Ferraro
 * @since 1.2
 */
public class SynchronizationContext
{
	private static Logger logger = LoggerFactory.getLogger(SynchronizationContext.class);
	
	private Collection<Database> activeDatabases;
	private Database sourceDatabase;
	private Database targetDatabase;
	private DatabaseCluster cluster;
	private Map<Database, Connection> connectionMap = new HashMap<Database, Connection>();
	
	public SynchronizationContext(DatabaseCluster cluster, Database database)
	{
		this.cluster = cluster;
		this.sourceDatabase = cluster.getBalancer().next();
		this.activeDatabases = cluster.getBalancer().list();
		this.targetDatabase = database;
	}
	
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
	
	public Database getSourceDatabase()
	{
		return this.sourceDatabase;
	}
	
	public Database getTargetDatabase()
	{
		return this.targetDatabase;
	}
	
	public Collection<Database> getActiveDatabases()
	{
		return this.activeDatabases;
	}
	
	public DatabaseMetaDataCache getDatabaseMetaDataCache()
	{
		return this.cluster.getDatabaseMetaDataCache();
	}
	
	public Dialect getDialect()
	{
		return this.cluster.getDialect();
	}
	
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
	}
	
}