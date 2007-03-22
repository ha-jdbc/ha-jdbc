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
	/**	Property that identifies this data source */
	public static final String DATABASE_CLUSTER = "cluster";
	
	private String cluster;
	
	/**
	 * @see javax.naming.Referenceable#getReference()
	 */
	public Reference getReference()
	{
		return new Reference(javax.sql.DataSource.class.getName(), new StringRefAddr(DATABASE_CLUSTER, this.cluster), this.getClass().getName(), null);
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
	 * @see javax.naming.spi.ObjectFactory#getObjectInstance(java.lang.Object, javax.naming.Name, javax.naming.Context, java.util.Hashtable)
	 */
	public Object getObjectInstance(Object object, Name name, Context context, Hashtable<?,?> environment) throws Exception
	{
		if (object == null) return null;
		
		if (!Reference.class.isInstance(object)) return null;
		
		Reference reference = Reference.class.cast(object);
		
		String className = reference.getClassName();
		
		if (className == null) return null;
		
		if (!javax.sql.DataSource.class.getName().equals(className)) return null;
		
		RefAddr addr = reference.get(DataSource.DATABASE_CLUSTER);
		
		if (addr == null) return null;
		
		String id = String.class.cast(addr.getContent());

		if (id == null) return null;
		
		DatabaseCluster<javax.sql.DataSource> cluster = DatabaseClusterFactory.getInstance().getDataSourceDatabaseClusterMap().get(id);
		
		if (cluster == null) return null;
		
		return ProxyFactory.createProxy(javax.sql.DataSource.class, new DataSourceInvocationHandler(cluster));
	}	
}
