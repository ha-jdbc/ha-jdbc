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
import java.util.Hashtable;

import javax.naming.Context;
import javax.naming.Name;
import javax.naming.RefAddr;
import javax.naming.Reference;
import javax.naming.spi.ObjectFactory;

import net.sf.hajdbc.Database;
import net.sf.hajdbc.DatabaseCluster;
import net.sf.hajdbc.DatabaseClusterConfiguration;
import net.sf.hajdbc.DatabaseClusterConfigurationFactory;
import net.sf.hajdbc.util.reflect.ProxyFactory;
import net.sf.hajdbc.xml.XMLDatabaseClusterConfigurationFactory;

/**
 * @author Paul Ferraro
 *
 * @param <Z>
 * @param <D>
 */
public abstract class CommonDataSourceObjectFactory<Z extends javax.sql.CommonDataSource, D extends Database<Z>> implements ObjectFactory, CommonDataSourceFactory<Z, D>
{
	private static final long serialVersionUID = 4537163442336124817L;
	
	private final Class<Z> targetClass;
	private final Class<? extends DatabaseClusterConfiguration<Z, D>> configurationClass;
	
	/**
	 * @param targetClass
	 */
	protected CommonDataSourceObjectFactory(Class<Z> targetClass, Class<? extends DatabaseClusterConfiguration<Z, D>> configurationClass)
	{
		this.targetClass = targetClass;
		this.configurationClass = configurationClass;
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
		
		DatabaseClusterConfigurationFactory<Z, D> factory = new XMLDatabaseClusterConfigurationFactory<Z, D>(id, config);
		
		return this.createProxy(id, factory);
	}
	
	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.sql.CommonDataSourceFactory#createProxy(java.lang.String, java.lang.String)
	 */
	@Override
	public Z createProxy(String id, DatabaseClusterConfigurationFactory<Z, D> factory) throws SQLException
	{
		DatabaseClusterConfiguration<Z, D> configuration = factory.createConfiguration(this.configurationClass);
		DatabaseCluster<Z, D> cluster = new DatabaseClusterImpl<Z, D>(id, configuration, factory);
		
		try
		{
			cluster.start();
		}
		catch (Exception e)
		{
			cluster.stop();

			throw SQLExceptionFactory.getInstance().createException(e);
		}
		
		return ProxyFactory.createProxy(this.targetClass, this.getInvocationHandler(cluster));
	}

	/**
	 * @param cluster
	 * @return the appropriate proxy invocation handler for this datasource
	 */
	protected abstract InvocationHandler getInvocationHandler(DatabaseCluster<Z, D> cluster);
}
