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

import javax.sql.DataSource;

/**
 * @author Paul Ferraro
 */
public class DataSourceReference extends CommonDataSourceReference<DataSource>
{
	private static final long serialVersionUID = -3303639090879442549L;

	/**
	 * Constructs a reference to a DataSource for the specified cluster
	 * @param cluster a cluster identifier
	 */
	public DataSourceReference(String cluster)
	{
		this(cluster, null);
	}
	
	/**
	 * Constructs a reference to a DataSource for the specified cluster
	 * @param cluster a cluster identifier
	 * @param config the uri of the configuration file
	 */
	public DataSourceReference(String cluster, String config)
	{
		super(DataSource.class, DataSourceFactory.class, cluster, config);
	}
}
