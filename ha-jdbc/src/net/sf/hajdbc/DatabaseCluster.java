/*
 * HA-JDBC: High-Availability JDBC
 * Copyright (C) 2004 Paul Ferraro
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
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Contains a map of <code>Database</code> -&gt; database connection factory (i.e. Driver, DataSource, ConnectionPoolDataSource, XADataSource)
 * @author  Paul Ferraro
 * @version $Revision$
 * @since   1.0
 */
public abstract class DatabaseCluster implements DatabaseClusterMBean
{
	private static Log log = LogFactory.getLog(DatabaseCluster.class);
	
	/**
	 * @see net.sf.hajdbc.DatabaseClusterMBean#isActive(java.lang.String)
	 */
	public final boolean isAlive(String databaseId) throws java.sql.SQLException
	{
		return this.isAlive(this.getDatabase(databaseId));
	}

	/**
	 * @see net.sf.hajdbc.DatabaseClusterMBean#deactivate(java.lang.String)
	 */
	public final void deactivate(String databaseId) throws java.sql.SQLException
	{
		this.deactivate(this.getDatabase(databaseId));
	}

	/**
	 * @see net.sf.hajdbc.DatabaseClusterMBean#activate(java.lang.String, java.lang.String)
	 */
	public final void activate(String databaseId, String strategyClassName) throws java.sql.SQLException
	{
		Database database = this.getDatabase(databaseId);
		
		// If there are no active databases then we can't synchronize with anything
		if (this.getActiveDatabaseList().isEmpty())
		{
			this.activate(database);
		}
		else
		{
			try
			{
				Class strategyClass = Class.forName(strategyClassName);
				
				if (!DatabaseSynchronizationStrategy.class.isAssignableFrom(strategyClass))
				{
					throw new SQLException("Specified synchronization strategy does not implement " + DatabaseSynchronizationStrategy.class.getName());
				}
				
				DatabaseSynchronizationStrategy strategy = (DatabaseSynchronizationStrategy) strategyClass.newInstance();

				this.activate(database, strategy);
			}
			catch (ClassNotFoundException e)
			{
				throw new SQLException(e);
			}
			catch (InstantiationException e)
			{
				throw new SQLException(e);
			}
			catch (IllegalAccessException e)
			{
				throw new SQLException(e);
			}
		}
	}
	
	public final void activate(Database database, DatabaseSynchronizationStrategy strategy) throws java.sql.SQLException
	{
		Connection connection = null;
		ConnectionProxy connectionProxy = null;
		
		try
		{
			connection = database.connect(this.getConnectionFactory());
			connection.setAutoCommit(false);
			
			List tableList = new LinkedList();
			
			DatabaseMetaData databaseMetaData = connection.getMetaData();
			ResultSet resultSet = databaseMetaData.getTables(null, null, "%", new String[] { "TABLE" });
			
			while (resultSet.next())
			{
				String table = resultSet.getString("TABLE_NAME");
				tableList.add(table);
			}
			
			resultSet.close();
	
			Operation operation = new Operation()
			{
				public Object execute(Database database, Object sqlObject) throws java.sql.SQLException
				{
					return database.connect(DatabaseCluster.this.getConnectionFactory());
				}
			};
	
			connectionProxy = new ConnectionProxy(this.getConnectionFactory(), this.getConnectionFactory().executeWrite(operation));
			
			connectionProxy.setAutoCommit(false);
			connectionProxy.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
			
			// Lock all tables
			Statement statement = connectionProxy.createStatement();
			Iterator tables = tableList.iterator();
			
			while (tables.hasNext())
			{
				String table = (String) tables.next();
				
				statement.addBatch("SELECT count(*) FROM " + table);
			}
			
			statement.executeBatch();
			statement.close();
			
			strategy.synchronize(connection, connectionProxy, tableList);
	
			this.activate(database);
			
			// Release table locks
			connectionProxy.rollback();
		}
		finally
		{
			if ((connection != null) && !connection.isClosed())
			{
				try
				{
					connection.close();
				}
				catch (java.sql.SQLException e)
				{
					log.warn("Failed to close connection of database: " + database);
				}
			}
			
			if ((connectionProxy != null) && !connectionProxy.isClosed())
			{
				try
				{
					connectionProxy.close();
				}
				catch (java.sql.SQLException e)
				{
					log.warn("Failed to close connection to active databases");
				}
			}
		}
	}
	
	public final void activate(String databaseId) throws java.sql.SQLException
	{
		this.activate(this.getDatabase(databaseId));
	}
	
	public abstract boolean activate(Database database);
	
	public abstract boolean addDatabase(Database database);
	
	public abstract boolean removeDatabase(Database database);
	
	public abstract Database firstDatabase() throws java.sql.SQLException;
	
	public abstract Database nextDatabase() throws java.sql.SQLException;

	public abstract List getActiveDatabaseList() throws java.sql.SQLException;
	
	public abstract ConnectionFactoryProxy getConnectionFactory();
	
	public abstract boolean isAlive(Database database);
	
	public abstract boolean deactivate(Database database);

	public abstract DatabaseClusterDescriptor getDescriptor();
	
	public abstract Database getDatabase(String databaseId) throws java.sql.SQLException;
}
