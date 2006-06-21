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
package net.sf.hajdbc.cache;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import net.sf.hajdbc.DatabaseMetaDataCache;
import net.sf.hajdbc.ForeignKeyConstraint;
import net.sf.hajdbc.UniqueConstraint;

/**
 * DatabaseMetaDataCache implementation that eagerly caches data when first flushed.
 * 
 * @author Paul Ferraro
 * @since 1.2
 */
public class EagerDatabaseMetaDataCache implements DatabaseMetaDataCache
{
	private DatabaseProperties properties = new DatabaseProperties();
	private DatabaseMetaDataCache cache = new DatabaseMetaDataCacheImpl();
	private ReadWriteLock lock = new ReentrantReadWriteLock();
	
	/**
	 * @see net.sf.hajdbc.cache.LazyDatabaseMetaDataCache#flush()
	 */
	public void flush() throws SQLException
	{
		this.lock.writeLock().lock();
		
		try
		{
			this.cache.flush();
			this.properties.clear();
			
			this.properties.setTablesMap(this.cache.getTables());
			this.properties.setSupportsSelectForUpdate(this.cache.supportsSelectForUpdate());
			
			for (Map.Entry<String, Collection<String>> tableMapEntry: this.properties.getTablesMap().entrySet())
			{
				String schema = tableMapEntry.getKey();
				
				for (String table: tableMapEntry.getValue())
				{
					TableProperties properties = this.properties.getTableProperties(schema, table);
					
					properties.setQualifiedNameForDDL(this.cache.getQualifiedNameForDDL(schema, table));
					properties.setQualifiedNameForDML(this.cache.getQualifiedNameForDML(schema, table));
					
//					String qualifiedTable = properties.getQualifiedTableForDML();
					
//					this.properties.getContainsAutoIncrementColumnMap().put(qualifiedTable, this.cache.containsAutoIncrementColumn(qualifiedTable));
					
					properties.setColumns(this.cache.getColumns(schema, table));
					properties.setForeignKeyConstraints(this.cache.getForeignKeyConstraints(schema, table));
					properties.setPrimaryKey(this.cache.getPrimaryKey(schema, table));
					properties.setUniqueConstraints(this.cache.getUniqueConstraints(schema, table));
				}
			}
		}
		finally
		{
			this.lock.writeLock().unlock();
		}
	}

	/**
	 * @see net.sf.hajdbc.DatabaseMetaDataCache#setConnection(java.sql.Connection)
	 */
	public void setConnection(Connection connection)
	{
		this.cache.setConnection(connection);
	}

	/**
	 * @see net.sf.hajdbc.DatabaseMetaDataCache#getTables()
	 */
	public Map<String, Collection<String>> getTables()
	{
		this.lock.readLock().lock();
		
		try
		{
			return this.properties.getTablesMap();
		}
		finally
		{
			this.lock.readLock().unlock();
		}
	}

	/**
	 * @see net.sf.hajdbc.DatabaseMetaDataCache#getPrimaryKey(java.lang.String, java.lang.String)
	 */
	public UniqueConstraint getPrimaryKey(String schema, String table)
	{
		this.lock.readLock().lock();
		
		try
		{
			return this.properties.getTableProperties(schema, table).getPrimaryKey();
		}
		finally
		{
			this.lock.readLock().unlock();
		}
	}

	/**
	 * @see net.sf.hajdbc.DatabaseMetaDataCache#getForeignKeyConstraints(java.lang.String, java.lang.String)
	 */
	public Collection<ForeignKeyConstraint> getForeignKeyConstraints(String schema, String table)
	{
		this.lock.readLock().lock();
		
		try
		{
			return this.properties.getTableProperties(schema, table).getForeignKeyConstraints();
		}
		finally
		{
			this.lock.readLock().unlock();
		}
	}

	/**
	 * @see net.sf.hajdbc.DatabaseMetaDataCache#getUniqueConstraints(java.lang.String, java.lang.String)
	 */
	public Collection<UniqueConstraint> getUniqueConstraints(String schema, String table)
	{
		this.lock.readLock().lock();
		
		try
		{
			return this.properties.getTableProperties(schema, table).getUniqueConstraints();
		}
		finally
		{
			this.lock.readLock().unlock();
		}
	}

	/**
	 * @see net.sf.hajdbc.DatabaseMetaDataCache#getColumns(java.lang.String, java.lang.String)
	 */
	public Map<String, ColumnProperties> getColumns(String schema, String table)
	{
		this.lock.readLock().lock();
		
		try
		{
			return this.properties.getTableProperties(schema, table).getColumnMap();
		}
		finally
		{
			this.lock.readLock().unlock();
		}
	}

	/**
	 * @see net.sf.hajdbc.DatabaseMetaDataCache#getQualifiedNameForDDL(java.lang.String, java.lang.String)
	 */
	public String getQualifiedNameForDDL(String schema, String table)
	{
		this.lock.readLock().lock();
		
		try
		{
			return this.properties.getTableProperties(schema, table).getQualifiedNameForDDL();
		}
		finally
		{
			this.lock.readLock().unlock();
		}
	}

	/**
	 * @see net.sf.hajdbc.DatabaseMetaDataCache#getQualifiedNameForDML(java.lang.String, java.lang.String)
	 */
	public String getQualifiedNameForDML(String schema, String table)
	{
		this.lock.readLock().lock();
		
		try
		{
			return this.properties.getTableProperties(schema, table).getQualifiedNameForDML();
		}
		finally
		{
			this.lock.readLock().unlock();
		}
	}

	/**
	 * @see net.sf.hajdbc.DatabaseMetaDataCache#supportsSelectForUpdate()
	 */
	public boolean supportsSelectForUpdate()
	{
		this.lock.readLock().lock();
		
		try
		{
			return this.properties.getSupportsSelectForUpdate();
		}
		finally
		{
			this.lock.readLock().unlock();
		}
	}

	/**
	 * @see net.sf.hajdbc.DatabaseMetaDataCache#containsAutoIncrementColumn(java.lang.String)
	 */
/*	public boolean containsAutoIncrementColumn(String qualifiedTable) throws SQLException
	{
		this.lock.readLock().lock();
		
		try
		{
			Map<String, Boolean> containsAutoIncrementColumnMap = this.properties.getContainsAutoIncrementColumnMap();
			
			Boolean containsAutoIncrementColumn = this.properties.getContainsAutoIncrementColumnMap().get(qualifiedTable);
			
			if (containsAutoIncrementColumn == null)
			{
				lock.unlock();
				
				lock = this.lock.writeLock();
				
				lock.lock();
				
				containsAutoIncrementColumn = this.cache.containsAutoIncrementColumn(qualifiedTable);
				
				containsAutoIncrementColumnMap.put(qualifiedTable, containsAutoIncrementColumn);
			}
			
			return containsAutoIncrementColumn;
		}
		finally
		{
			this.lock.readLock().unlock();
		}
	}
*/
}
