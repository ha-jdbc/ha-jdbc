/*
 * HA-JDBC: High-Availability JDBC
 * Copyright (C) 2004 Paul Ferraro
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
package net.sf.hajdbc;

import javax.naming.Reference;
import javax.naming.Referenceable;
import javax.naming.StringRefAddr;

/**
 * @author  Paul Ferraro
 * @version $Revision$
 * @since   1.0
 */
public abstract class AbstractDataSource implements Referenceable
{
	public static final String CLUSTER_NAME = "name";
	
	protected ConnectionFactoryProxy connectionFactory;

	/**
	 * Returns the name of this DataSource
	 * @return the name of this DataSource
	 */
	public String getName()
	{
		return this.connectionFactory.getDatabaseCluster().getId();
	}
	
	/**
	 * Sets the name of this DataSource
	 * @param name the name of this DataSource
	 * @throws java.sql.SQLException
	 */
	public void setName(String name) throws java.sql.SQLException
	{
		this.connectionFactory = DatabaseClusterFactory.getInstance().getDatabaseCluster(name).getConnectionFactory();
	}
	
	/**
	 * @see javax.naming.Referenceable#getReference()
	 */
	public final Reference getReference()
	{
        Reference ref = new Reference(this.getClass().getName(), this.getObjectFactoryClass().getName(), null);
        
        ref.add(new StringRefAddr(CLUSTER_NAME, this.getName()));
        
        return ref;
	}
	
	/**
	 * Returns the implementation class for the factory that will create DataSources
	 * @return a class that implements javax.naming.spi.ObjectFactory
	 */
	protected abstract Class getObjectFactoryClass();
}
