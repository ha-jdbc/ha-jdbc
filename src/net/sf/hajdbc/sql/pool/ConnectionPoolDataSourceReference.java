/*
 * HA-JDBC: High-Availability JDBC
 * Copyright (c) 2004-2008 Paul Ferraro
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
package net.sf.hajdbc.sql.pool;

import javax.sql.ConnectionPoolDataSource;

import net.sf.hajdbc.sql.CommonDataSourceReference;

/**
 * @author Paul Ferraro
 */
public class ConnectionPoolDataSourceReference extends CommonDataSourceReference<ConnectionPoolDataSource>
{
	private static final long serialVersionUID = 2473805187473417008L;

	/**
	 * Constructs a reference to a <code>ConnectionPoolDataSource</code> for the specified cluster
	 * @param cluster a cluster identifier
	 */
	protected ConnectionPoolDataSourceReference(String cluster)
	{
		this(cluster, null);
	}
	
	/**
	 * Constructs a reference to a <code>ConnectionPoolDataSource</code> for the specified cluster
	 * @param cluster a cluster identifier
	 * @param config the uri of the configuration file
	 */
	protected ConnectionPoolDataSourceReference(String cluster, String config)
	{
		super(ConnectionPoolDataSource.class, ConnectionPoolDataSourceFactory.class, cluster, config);
	}
}
