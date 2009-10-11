/*
 * HA-JDBC: High-Availability JDBC
 * Copyright 2004-2009 Paul Ferraro
 * 
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 * 
 * You should have received a copy of the GNU Lesser General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
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
