/*
 * Copyright (c) 2004-2006, Identity Theft 911, LLC.  All rights reserved.
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
