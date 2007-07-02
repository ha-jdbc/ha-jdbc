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

import java.util.Hashtable;

import javax.naming.Context;
import javax.naming.Name;
import javax.naming.RefAddr;
import javax.naming.Reference;
import javax.naming.Referenceable;
import javax.naming.StringRefAddr;
import javax.naming.spi.ObjectFactory;

import net.sf.hajdbc.DatabaseCluster;
import net.sf.hajdbc.DatabaseClusterFactory;
import net.sf.hajdbc.util.reflect.ProxyFactory;


/**
 * @author Paul Ferraro
 * @version $Revision$
 */
public class DataSource implements Referenceable, ObjectFactory
{
	private static final String CLUSTER = "cluster";
	private static final String CONFIG = "config";
	
	private String cluster;
	private String config;
	
	/**
	 * @see javax.naming.Referenceable#getReference()
	 */
	@Override
	public Reference getReference()
	{
		Reference reference = new Reference(javax.sql.DataSource.class.getName(), this.getClass().getName(), null);
		
		reference.add(new StringRefAddr(CLUSTER, this.cluster));
		reference.add(new StringRefAddr(CONFIG, this.config));
		
		return reference;
	}
	
	/**
	 * Returns the identifier of the database cluster represented by this DataSource
	 * @return a database cluster identifier
	 */
	public String getCluster()
	{
		return this.cluster;
	}
	
	/**
	 * Sets the identifier of the database cluster represented by this DataSource
	 * @param cluster a database cluster identifier
	 */
	public void setCluster(String cluster)
	{
		this.cluster = cluster;
	}

	/**
	 * Returns the resource name of the configuration file used to load the database cluster represented by this DataSource.
	 * @return a resource name
	 */
	public String getConfig()
	{
		return this.config;
	}
	
	/**
	 * Sets the resource name of the configuration file used to load the database cluster represented by this DataSource.
	 * @param config a resource name
	 */
	public void setConfig(String config)
	{
		this.config = config;
	}
	
	/**
	 * @see javax.naming.spi.ObjectFactory#getObjectInstance(java.lang.Object, javax.naming.Name, javax.naming.Context, java.util.Hashtable)
	 */
	@Override
	public Object getObjectInstance(Object object, Name name, Context context, Hashtable<?,?> environment) throws Exception
	{
		if ((object == null) || !(object instanceof Reference)) return null;
		
		Reference reference = (Reference) object;
		
		String className = reference.getClassName();
		
		if ((className == null) || !className.equals(javax.sql.DataSource.class.getName())) return null;
		
		RefAddr idAddr = reference.get(CLUSTER);
		
		if (idAddr == null) return null;
		
		Object idAddrContent = idAddr.getContent();
		
		if ((idAddrContent == null) || !(idAddrContent instanceof String)) return null;
		
		String id = (String) idAddrContent;
		
		RefAddr configAddr = reference.get(CONFIG);
		
		String config = null;
		
		if (configAddr != null)
		{
			Object configAddrContent = configAddr.getContent();
			
			if ((configAddrContent != null) && (configAddrContent instanceof String))
			{
				config = (String) configAddrContent;
			}
		}
		
		DatabaseCluster<javax.sql.DataSource> cluster = DatabaseClusterFactory.getDatabaseCluster(id, DataSourceDatabaseCluster.class, DataSourceDatabaseClusterMBean.class, config);
		
		if (cluster == null) return null;
		
		return ProxyFactory.createProxy(javax.sql.DataSource.class, new DataSourceInvocationHandler(cluster));
	}	
}
