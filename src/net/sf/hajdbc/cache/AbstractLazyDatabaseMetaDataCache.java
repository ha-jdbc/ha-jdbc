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

import net.sf.hajdbc.DatabaseMetaDataCache;
import net.sf.hajdbc.ForeignKeyConstraint;
import net.sf.hajdbc.UniqueConstraint;

/**
 * @author Paul Ferraro
 *
 */
public abstract class AbstractLazyDatabaseMetaDataCache implements DatabaseMetaDataCache
{
	private DatabaseMetaDataCache cache = new DatabaseMetaDataCacheImpl();
	
	protected abstract DatabaseProperties getDatabaseProperties();
	
	/**
	 * @see net.sf.hajdbc.DatabaseMetaDataCache#flush()
	 */
	public synchronized void flush() throws SQLException
	{
		this.cache.flush();
		
		this.getDatabaseProperties().clear();
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
	public Map<String, Collection<String>> getTables() throws SQLException
	{
		Map<String, Collection<String>> tablesMap = this.getDatabaseProperties().getTablesMap();
		
		if (tablesMap == null)
		{
			tablesMap = this.cache.getTables();
			
			this.getDatabaseProperties().setTablesMap(tablesMap);
		}
		
		return tablesMap;
	}
	
	/**
	 * @see net.sf.hajdbc.DatabaseMetaDataCache#getPrimaryKey(java.lang.String, java.lang.String)
	 */
	public UniqueConstraint getPrimaryKey(String schema, String table) throws SQLException
	{
		TableProperties properties = this.getDatabaseProperties().getTableProperties(schema, table);
		
		UniqueConstraint constraint = properties.getPrimaryKey();
		
		if (constraint == null)
		{
			constraint = this.cache.getPrimaryKey(schema, table);
			
			properties.setPrimaryKey(constraint);
		}
		
		return constraint;
	}

	/**
	 * @see net.sf.hajdbc.DatabaseMetaDataCache#getForeignKeyConstraints(java.lang.String, java.lang.String)
	 */
	public Collection<ForeignKeyConstraint> getForeignKeyConstraints(String schema, String table) throws SQLException
	{
		TableProperties properties = this.getDatabaseProperties().getTableProperties(schema, table);
		
		Collection<ForeignKeyConstraint> constraints = properties.getForeignKeyConstraints();
		
		if (constraints == null)
		{
			constraints = this.cache.getForeignKeyConstraints(schema, table);
			
			properties.setForeignKeyConstraints(constraints);
		}
		
		return constraints;
	}

	/**
	 * @see net.sf.hajdbc.DatabaseMetaDataCache#getUniqueConstraints(java.lang.String, java.lang.String)
	 */
	public Collection<UniqueConstraint> getUniqueConstraints(String schema, String table) throws SQLException
	{
		TableProperties properties = this.getDatabaseProperties().getTableProperties(schema, table);
		
		Collection<UniqueConstraint> constraints = properties.getUniqueConstraints();
		
		if (constraints == null)
		{
			constraints = this.cache.getUniqueConstraints(schema, table);
			
			properties.setUniqueConstraints(constraints);
		}
		
		return constraints;
	}

	/**
	 * @see net.sf.hajdbc.DatabaseMetaDataCache#getColumns(java.lang.String, java.lang.String)
	 */
	public Map<String, ColumnProperties> getColumns(String schema, String table) throws SQLException
	{
		TableProperties properties = this.getDatabaseProperties().getTableProperties(schema, table);
		
		Map<String, ColumnProperties> columnMap = properties.getColumnMap();
		
		if (columnMap == null)
		{
			columnMap = this.cache.getColumns(schema, table);
			
			properties.setColumns(columnMap);
		}
		
		return columnMap;
	}

	/**
	 * @see net.sf.hajdbc.DatabaseMetaDataCache#getQualifiedTableForDDL(java.lang.String, java.lang.String)
	 */
	public String getQualifiedTableForDDL(String schema, String table) throws SQLException
	{
		TableProperties properties = this.getDatabaseProperties().getTableProperties(schema, table);
		
		String qualifiedTable = properties.getQualifiedTableForDDL();
		
		if (qualifiedTable == null)
		{
			qualifiedTable = this.cache.getQualifiedTableForDDL(schema, table);
			
			properties.setQualifiedTableForDDL(qualifiedTable);
		}
		
		return qualifiedTable;
	}

	/**
	 * @see net.sf.hajdbc.DatabaseMetaDataCache#getQualifiedTableForDML(java.lang.String, java.lang.String)
	 */
	public String getQualifiedTableForDML(String schema, String table) throws SQLException
	{
		TableProperties properties = this.getDatabaseProperties().getTableProperties(schema, table);
		
		String qualifiedTable = properties.getQualifiedTableForDML();
		
		if (qualifiedTable == null)
		{
			qualifiedTable = this.cache.getQualifiedTableForDML(schema, table);
			
			properties.setQualifiedTableForDML(qualifiedTable);
		}
		
		return qualifiedTable;
	}

	/**
	 * @see net.sf.hajdbc.DatabaseMetaDataCache#supportsSelectForUpdate()
	 */
	public boolean supportsSelectForUpdate() throws SQLException
	{
		Boolean supportsSelectForUpdate = this.getDatabaseProperties().getSupportsSelectForUpdate();
		
		if (supportsSelectForUpdate == null)
		{
			supportsSelectForUpdate = this.cache.supportsSelectForUpdate();
			
			this.getDatabaseProperties().setSupportsSelectForUpdate(supportsSelectForUpdate);
		}
		
		return supportsSelectForUpdate;
	}

	/**
	 * @see net.sf.hajdbc.DatabaseMetaDataCache#containsAutoIncrementColumn(java.lang.String)
	 */
	public boolean containsAutoIncrementColumn(String qualifiedTable) throws SQLException
	{
		Map<String, Boolean> containsAutoIncrementColumnMap = this.getDatabaseProperties().getContainsAutoIncrementColumnMap();
		
		Boolean containsAutoIncrementColumn = containsAutoIncrementColumnMap.get(qualifiedTable);
		
		if (containsAutoIncrementColumn == null)
		{
			containsAutoIncrementColumn = this.cache.containsAutoIncrementColumn(qualifiedTable);
			
			containsAutoIncrementColumnMap.put(qualifiedTable, containsAutoIncrementColumn);
		}
		
		return containsAutoIncrementColumn;
	}
}
