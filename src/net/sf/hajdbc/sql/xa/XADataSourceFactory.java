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
package net.sf.hajdbc.sql.xa;

import java.lang.reflect.InvocationHandler;
import java.sql.SQLException;

import javax.sql.XADataSource;

import net.sf.hajdbc.DatabaseCluster;
import net.sf.hajdbc.DatabaseClusterFactory;
import net.sf.hajdbc.sql.CommonDataSourceObjectFactory;

/**
 * @author Paul Ferraro
 */
public class XADataSourceFactory extends CommonDataSourceObjectFactory<XADataSource>
{
	/**
	 * Constructs a new factory for creating an <code>XADataSource</code>.
	 */
	public XADataSourceFactory()
	{
		super(XADataSource.class);
	}

	/**
	 * @see net.sf.hajdbc.sql.CommonDataSourceObjectFactory#getDatabaseCluster(java.lang.String, java.lang.String)
	 */
	@Override
	protected DatabaseCluster<javax.sql.XADataSource> getDatabaseCluster(String id, String config) throws SQLException
	{
		return DatabaseClusterFactory.getDatabaseCluster(id, XADataSourceDatabaseCluster.class, XADataSourceDatabaseClusterMBean.class, config);
	}

	/**
	 * @see net.sf.hajdbc.sql.CommonDataSourceObjectFactory#getInvocationHandler(net.sf.hajdbc.DatabaseCluster)
	 */
	@Override
	protected InvocationHandler getInvocationHandler(DatabaseCluster<XADataSource> cluster)
	{
		return new XADataSourceInvocationHandler(cluster);
	}
}
