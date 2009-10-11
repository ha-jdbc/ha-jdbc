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

import java.lang.reflect.InvocationHandler;
import java.sql.SQLException;

import javax.sql.DataSource;

import net.sf.hajdbc.DatabaseCluster;

/**
 * @author Paul Ferraro
 */
public class DataSourceFactory extends CommonDataSourceObjectFactory<DataSource, DataSourceDatabase>
{
	
	/**
	 * Constructs a new factory for creating a <code>DataSource</code>.
	 */
	public DataSourceFactory()
	{
		super(javax.sql.DataSource.class);
	}

	@Override
	protected DatabaseCluster<DataSource, DataSourceDatabase> getDatabaseCluster(String id, String config) throws SQLException
	{
		return null; //DatabaseClusterFactory.getDatabaseCluster(id, DataSourceDatabaseCluster.class, DataSourceDatabaseClusterMBean.class, config);
	}

	@Override
	protected InvocationHandler getInvocationHandler(DatabaseCluster<DataSource, DataSourceDatabase> cluster)
	{
		return new DataSourceInvocationHandler(cluster);
	}	
}
