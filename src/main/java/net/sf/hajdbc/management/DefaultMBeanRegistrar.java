/*
 * HA-JDBC: High-Availability JDBC
 * Copyright (C) 2012  Paul Ferraro
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
package net.sf.hajdbc.management;

import java.lang.management.ManagementFactory;
import java.util.Hashtable;

import javax.management.JMException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import net.sf.hajdbc.Database;
import net.sf.hajdbc.DatabaseCluster;
import net.sf.hajdbc.logging.Level;
import net.sf.hajdbc.logging.Logger;
import net.sf.hajdbc.logging.LoggerFactory;

/**
 * @author Paul Ferraro
 */
public class DefaultMBeanRegistrar<Z, D extends Database<Z>> implements MBeanRegistrar<Z, D>
{
	private static final String TYPE_ATTRIBUTE = "type";
	private static final String CLUSTER_ATTRIBUTE = "cluster";
	private static final String DATABASE_ATTRIBUTE = "database";
	
	private static final String CLUSTER_TYPE = "DatabaseCluster";
	private static final String DATABASE_TYPE = "Database";
	
	private static final Logger logger = LoggerFactory.getLogger(DefaultMBeanRegistrar.class);
	
	private final MBeanServer server;
	private String domain = DatabaseCluster.class.getPackage().getName();
	
	public DefaultMBeanRegistrar()
	{
		this(ManagementFactory.getPlatformMBeanServer());
	}
	
	public DefaultMBeanRegistrar(MBeanServer server)
	{
		this.server = server;
	}
	
	public void setDomain(String domain)
	{
		this.domain = domain;
	}
	
	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.management.MBeanRegistrar#register(net.sf.hajdbc.DatabaseCluster)
	 */
	@Override
	public void register(DatabaseCluster<Z, D> cluster) throws JMException
	{
		this.register(cluster, this.createAttributes(cluster));
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.management.MBeanRegistrar#register(net.sf.hajdbc.DatabaseCluster, net.sf.hajdbc.Database)
	 */
	@Override
	public void register(DatabaseCluster<Z, D> cluster, D database) throws JMException
	{
		this.register(database, this.createAttributes(cluster, database));
	}

	private void register(Object object, Hashtable<String, String> attributes) throws JMException
	{
		ObjectName name = this.createObjectName(attributes);
		
		this.server.registerMBean(new AnnotatedMBean(object), name);
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.management.MBeanRegistrar#unregister(net.sf.hajdbc.DatabaseCluster)
	 */
	@Override
	public void unregister(DatabaseCluster<Z, D> cluster)
	{
		this.unregister(this.createAttributes(cluster));
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.management.MBeanRegistrar#unregister(net.sf.hajdbc.DatabaseCluster, net.sf.hajdbc.Database)
	 */
	@Override
	public void unregister(DatabaseCluster<Z, D> cluster, D database)
	{
		this.unregister(this.createAttributes(cluster, database));
	}

	private void unregister(Hashtable<String, String> attributes)
	{
		try
		{
			ObjectName name = this.createObjectName(attributes);
			
			if (this.server.isRegistered(name))
			{
				this.server.unregisterMBean(name);
			}
		}
		catch (Exception e)
		{
			logger.log(Level.WARN, e);
		}
	}

	private Hashtable<String, String> createAttributes(DatabaseCluster<Z, D> cluster)
	{
		Hashtable<String, String> attributes = new Hashtable<String, String>();
		attributes.put(TYPE_ATTRIBUTE, CLUSTER_TYPE);
		attributes.put(CLUSTER_ATTRIBUTE, cluster.getId());
		return attributes;
	}

	private Hashtable<String, String> createAttributes(DatabaseCluster<Z, D> cluster, D database)
	{
		Hashtable<String, String> attributes = new Hashtable<String, String>();
		attributes.put(TYPE_ATTRIBUTE, DATABASE_TYPE);
		attributes.put(CLUSTER_ATTRIBUTE, cluster.getId());
		attributes.put(DATABASE_ATTRIBUTE, database.getId());
		return attributes;
	}
	
	private ObjectName createObjectName(Hashtable<String, String> attributes) throws MalformedObjectNameException
	{
		return ObjectName.getInstance(this.domain, attributes);
	}
}
