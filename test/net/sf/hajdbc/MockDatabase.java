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
package net.sf.hajdbc;

import java.sql.Connection;
import java.sql.SQLException;

import net.sf.hajdbc.sql.AbstractDatabase;

/**
 * @author Paul Ferraro
 *
 */
public class MockDatabase extends AbstractDatabase<Void>
{
	public MockDatabase()
	{
		this("");
	}
	
	public MockDatabase(String id)
	{
		this(id, 1);
	}
	
	public MockDatabase(String id, int weight)
	{
		this.id = id;
		this.weight = weight;
	}

	/**
	 * @see net.sf.hajdbc.Database#connect(T)
	 */
	public Connection connect(Void connectionFactory) throws SQLException
	{
		return null;
	}

	/**
	 * @see net.sf.hajdbc.Database#createConnectionFactory()
	 */
	public Void createConnectionFactory()
	{
		return null;
	}

	/**
	 * @see net.sf.hajdbc.Database#getConnectionFactoryClass()
	 */
	public Class<Void> getConnectionFactoryClass()
	{
		return null;
	}

	/**
	 * @see net.sf.hajdbc.Database#getActiveMBeanClass()
	 */
	public Class<? extends ActiveDatabaseMBean> getActiveMBeanClass()
	{
		return null;
	}

	/**
	 * @see net.sf.hajdbc.Database#getInactiveMBeanClass()
	 */
	public Class<? extends InactiveDatabaseMBean> getInactiveMBeanClass()
	{
		return null;
	}
}
