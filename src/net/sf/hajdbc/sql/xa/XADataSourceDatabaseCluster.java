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

import java.net.URL;

import javax.sql.XADataSource;

import org.jibx.runtime.IUnmarshallingContext;

import net.sf.hajdbc.sql.CommonDataSourceDatabase;
import net.sf.hajdbc.sql.CommonDataSourceDatabaseCluster;

/**
 * @author Paul Ferraro
 *
 */
public class XADataSourceDatabaseCluster extends CommonDataSourceDatabaseCluster<XADataSource> implements XADataSourceDatabaseClusterMBean
{
	/**
	 * Object factory for JiBX that pulls the cluster instance from the unmarshalling context
	 * @param context unmarshalling context
	 * @return a database cluster
	 */
	public static XADataSourceDatabaseCluster extractDatabaseCluster(IUnmarshallingContext context)
	{
		return (XADataSourceDatabaseCluster) context.getUserContext();
	}
	
	/**
	 * @param id
	 * @param url
	 */
	public XADataSourceDatabaseCluster(String id, URL url)
	{
		super(id, url);
	}

	/**
	 * @see net.sf.hajdbc.sql.CommonDataSourceDatabaseCluster#createDatabase()
	 */
	@Override
	protected CommonDataSourceDatabase<XADataSource> createDatabase()
	{
		return new XADataSourceDatabase();
	}
}
