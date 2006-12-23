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
package net.sf.hajdbc.sql;

import javax.sql.DataSource;

import net.sf.hajdbc.DatabaseClusterMBean;

/**
 * @author Paul Ferraro
 *
 */
public class DataSourceDatabaseCluster extends AbstractDatabaseCluster<DataSource> implements DataSourceDatabaseClusterMBean
{
	/**
	 * @see net.sf.hajdbc.sql.AbstractDatabaseCluster#getMBeanClass()
	 */
	@Override
	protected Class<? extends DatabaseClusterMBean> getMBeanClass()
	{
		return null;
	}

	/**
	 * @see net.sf.hajdbc.sql.DataSourceDatabaseClusterMBean#add(java.lang.String, java.lang.String)
	 */
	public void add(String databaseId, String name)
	{
		DataSourceDatabase database = new DataSourceDatabase();
		
		database.setId(databaseId);
		database.setName(name);
		
		this.register(database, database.getInactiveMBeanClass());
		
		this.add(database);
	}
}
