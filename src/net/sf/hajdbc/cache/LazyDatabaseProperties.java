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
package net.sf.hajdbc.cache;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import net.sf.hajdbc.DatabaseProperties;
import net.sf.hajdbc.Dialect;
import net.sf.hajdbc.TableProperties;

/**
 * @author Paul Ferraro
 *
 */
public class LazyDatabaseProperties implements DatabaseProperties
{
	private static ThreadLocal<Connection> threadLocal = new ThreadLocal<Connection>();
	
	private Map<String, TableProperties> tableMap;
	private Boolean supportsSelectForUpdate;
	private DatabaseMetaDataSupport support;
	private List<String> defaultSchemaList;
	private Collection<String> sequences;
	private Dialect dialect;

	public LazyDatabaseProperties(Dialect dialect) throws SQLException
	{
		this.support = new DatabaseMetaDataSupport(getDatabaseMetaData());
		this.dialect = dialect;
	}

	public static void setConnection(Connection connection)
	{
		threadLocal.set(connection);
	}
	
	public static DatabaseMetaData getDatabaseMetaData() throws SQLException
	{
		return threadLocal.get().getMetaData();
	}
	
	/**
	 * @see net.sf.hajdbc.DatabaseProperties#getTables()
	 */
	@Override
	public synchronized Collection<TableProperties> getTables() throws SQLException
	{
		return this.getTableMap().values();
	}

	private synchronized Map<String, TableProperties> getTableMap() throws SQLException
	{
		DatabaseMetaData metaData = getDatabaseMetaData();
		
		if (this.tableMap == null)
		{
			this.tableMap = new HashMap<String, TableProperties>();
			
			Map<String, Collection<String>> tablesMap = this.support.getTables(metaData);
			
			for (Map.Entry<String, Collection<String>> tablesMapEntry: tablesMap.entrySet())
			{
				String schema = tablesMapEntry.getKey();
				Collection<String> tables = tablesMapEntry.getValue();
				
				for (String table: tables)
				{
					TableProperties properties = new LazyTableProperties(this.support, schema, table);
					
					this.tableMap.put(properties.getName(), properties);
				}
			}
		}
		
		return this.tableMap;
	}
	
	private synchronized List<String> getDefaultSchemaList() throws SQLException
	{
		if (this.defaultSchemaList == null)
		{
			this.defaultSchemaList = this.dialect.getDefaultSchemas(threadLocal.get());
		}
		
		return this.defaultSchemaList;
	}
	
	/**
	 * @see net.sf.hajdbc.DatabaseProperties#findTable(java.lang.String)
	 */
	@Override
	public TableProperties findTable(String table) throws SQLException
	{
		return this.support.findTable(this.getTableMap(), table, this.getDefaultSchemaList(), this.dialect);
	}

	/**
	 * @see net.sf.hajdbc.DatabaseProperties#supportsSelectForUpdate()
	 */
	@Override
	public synchronized boolean supportsSelectForUpdate() throws SQLException
	{
		if (this.supportsSelectForUpdate == null)
		{
			this.supportsSelectForUpdate = getDatabaseMetaData().supportsSelectForUpdate();
		}
		
		return this.supportsSelectForUpdate;
	}

	/**
	 * @see net.sf.hajdbc.DatabaseProperties#getSequences()
	 */
	@Override
	public synchronized Collection<String> getSequences() throws SQLException
	{
		if (this.sequences == null)
		{
			this.sequences = this.dialect.getSequences(threadLocal.get());
		}
		
		return this.sequences;
	}
}
