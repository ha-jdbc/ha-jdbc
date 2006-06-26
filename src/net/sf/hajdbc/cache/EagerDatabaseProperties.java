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
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import net.sf.hajdbc.DatabaseProperties;
import net.sf.hajdbc.TableProperties;

/**
 * @author Paul Ferraro
 *
 */
public class EagerDatabaseProperties implements DatabaseProperties
{
	private List<TableProperties> tableList = new LinkedList<TableProperties>();
	private boolean supportsSelectForUpdate;
	
	public EagerDatabaseProperties(Connection connection) throws SQLException
	{
		DatabaseMetaData metaData = connection.getMetaData();
		DatabaseMetaDataSupport support = new DatabaseMetaDataSupport(metaData);
		
		this.supportsSelectForUpdate = metaData.supportsSelectForUpdate();
		
		Map<String, Collection<String>> tablesMap = support.getTables(metaData);
		
		for (Map.Entry<String, Collection<String>> tablesMapEntry: tablesMap.entrySet())
		{
			String schema = tablesMapEntry.getKey();
			Collection<String> tables = tablesMapEntry.getValue();
			
			for (String table: tables)
			{
				this.tableList.add(new EagerTableProperties(metaData, support, schema, table));
			}
		}
	}

	/**
	 * @see net.sf.hajdbc.DatabaseProperties#getSchemas()
	 */
	public Collection<TableProperties> getTables()
	{
		return this.tableList;
	}

	/**
	 * @see net.sf.hajdbc.DatabaseProperties#getSupportsSelectForUpdate()
	 */
	public boolean getSupportsSelectForUpdate()
	{
		return this.supportsSelectForUpdate;
	}	
}
