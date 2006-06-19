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

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * @author Paul Ferraro
 *
 */
public class DatabaseProperties
{
	private Map<String, Collection<String>> tablesMap = null;
	private Map<String, Map<String, TableProperties>> tablePropertiesMap = new HashMap<String, Map<String, TableProperties>>();
	private Map<String, Boolean> containsAutoIncrementColumnMap = new HashMap<String, Boolean>();
	private Boolean supportsSelectForUpdate;
	
	public Map<String, Collection<String>> getTablesMap()
	{
		return this.tablesMap;
	}

	public void setTablesMap(Map<String, Collection<String>> tablesMap)
	{
		this.tablesMap = tablesMap;
	}
	
	public Map<String, Boolean> getContainsAutoIncrementColumnMap()
	{
		return this.containsAutoIncrementColumnMap;
	}
	
	public void clear()
	{
		this.tablesMap = null;
		this.tablePropertiesMap.clear();
		this.containsAutoIncrementColumnMap.clear();
	}
	
	public TableProperties getTableProperties(String schema, String table)
	{
		Map<String, TableProperties> tableMap = this.tablePropertiesMap.get(schema);
		
		if (tableMap == null)
		{
			tableMap = new HashMap<String, TableProperties>();
			
			this.tablePropertiesMap.put(schema, tableMap);
		}
		
		TableProperties properties = tableMap.get(table);
		
		if (properties == null)
		{
			properties = new TableProperties();
			
			tableMap.put(table, properties);
		}
		
		return properties;
	}

	/**
	 * @return Returns the supportsSelectForUpdate.
	 */
	public Boolean getSupportsSelectForUpdate()
	{
		return this.supportsSelectForUpdate;
	}

	/**
	 * @param supportsSelectForUpdate The supportsSelectForUpdate to set.
	 */
	public void setSupportsSelectForUpdate(Boolean supportsSelectForUpdate)
	{
		this.supportsSelectForUpdate = supportsSelectForUpdate;
	}
}
