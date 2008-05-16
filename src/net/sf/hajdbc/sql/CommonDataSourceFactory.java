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
package net.sf.hajdbc.sql;

import java.lang.reflect.InvocationHandler;
import java.sql.SQLException;
import java.util.Hashtable;

import javax.naming.Context;
import javax.naming.Name;
import javax.naming.RefAddr;
import javax.naming.Reference;
import javax.naming.spi.ObjectFactory;
import javax.sql.CommonDataSource;

import net.sf.hajdbc.DatabaseCluster;
import net.sf.hajdbc.util.reflect.ProxyFactory;

/**
 * @author Paul Ferraro
 *
 * @param <D>
 */
public abstract class CommonDataSourceFactory<D extends CommonDataSource> implements ObjectFactory, DataSourceProxyFactory<D>
{
	private Class<D> targetClass;
	
	/**
	 * @param targetClass
	 */
	protected CommonDataSourceFactory(Class<D> targetClass)
	{
		this.targetClass = targetClass;
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
		
		if ((className == null) || !className.equals(this.targetClass.getName())) return null;
		
		RefAddr idAddr = reference.get(CommonDataSourceReference.CLUSTER);
		
		if (idAddr == null) return null;
		
		Object idAddrContent = idAddr.getContent();
		
		if ((idAddrContent == null) || !(idAddrContent instanceof String)) return null;
		
		String id = (String) idAddrContent;
		
		RefAddr configAddr = reference.get(CommonDataSourceReference.CONFIG);
		
		String config = null;
		
		if (configAddr != null)
		{
			Object configAddrContent = configAddr.getContent();
			
			if ((configAddrContent != null) && (configAddrContent instanceof String))
			{
				config = (String) configAddrContent;
			}
		}
		
		return this.createDataSource(id, config);
	}

	/**
	 * @see net.sf.hajdbc.sql.DataSourceProxyFactory#createDataSource(java.lang.String, java.lang.String)
	 */
	public D createDataSource(String id, String config) throws SQLException
	{
		DatabaseCluster<D> cluster = this.getDatabaseCluster(id, config);
		
		if (cluster == null) return null;
		
		return ProxyFactory.createProxy(this.targetClass, this.getInvocationHandler(cluster));
	}
	
	/**
	 * @param id
	 * @param config
	 * @return the appropriate database cluster
	 * @throws SQLException
	 */
	protected abstract DatabaseCluster<D> getDatabaseCluster(String id, String config) throws SQLException;
	
	/**
	 * @param cluster
	 * @return the appropriate proxy invocation handler for this datasource
	 */
	protected abstract InvocationHandler getInvocationHandler(DatabaseCluster<D> cluster);
}