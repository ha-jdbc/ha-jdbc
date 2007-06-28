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
package net.sf.hajdbc.sql;

import java.net.URL;

import javax.sql.DataSource;

import org.jibx.runtime.IUnmarshallingContext;

/**
 * @author Paul Ferraro
 *
 */
public class DataSourceDatabaseCluster extends AbstractDatabaseCluster<DataSource> implements DataSourceDatabaseClusterMBean
{
	public static DataSourceDatabaseCluster extractDatabaseCluster(IUnmarshallingContext context)
	{
		return (DataSourceDatabaseCluster) context.getUserContext();
	}
	
	/**
	 * Constructs a new DataSourceDatabaseCluster
	 * @param id
	 * @param url
	 */
	public DataSourceDatabaseCluster(String id, URL url)
	{
		super(id, url);
	}

	/**
	 * @see net.sf.hajdbc.sql.DataSourceDatabaseClusterMBean#add(java.lang.String, java.lang.String)
	 */
	public void add(String databaseId, String name)
	{
		DataSourceDatabase database = new DataSourceDatabase();
		
		database.setId(databaseId);
		database.setName(name);
		
		this.register(database, database.getInactiveMBean());
		
		this.add(database);
	}
}
