/*
 * Copyright (c) 2004-2007, Identity Theft 911, LLC.  All rights reserved.
 */
package net.sf.hajdbc.sql;

import java.sql.Connection;
import java.sql.SQLException;

import javax.management.DynamicMBean;
import javax.sql.DataSource;

/**
 * @author Paul Ferraro
 */
public class MockDataSourceDatabase extends AbstractDatabase<DataSource>
{
	private DataSource dataSource;
	
	public MockDataSourceDatabase(String id, DataSource dataSource)
	{
		this.setId(id);
		this.clean();
		this.dataSource = dataSource;
	}
	
	/**
	 * @see net.sf.hajdbc.Database#connect(java.lang.Object)
	 */
	@Override
	public Connection connect(DataSource dataSource) throws SQLException
	{
		return dataSource.getConnection();
	}

	/**
	 * @see net.sf.hajdbc.Database#createConnectionFactory()
	 */
	@Override
	public DataSource createConnectionFactory()
	{
		return this.dataSource;
	}

	/**
	 * @see net.sf.hajdbc.Database#getActiveMBean()
	 */
	@Override
	public DynamicMBean getActiveMBean()
	{
		return null;
	}

	/**
	 * @see net.sf.hajdbc.Database#getInactiveMBean()
	 */
	@Override
	public DynamicMBean getInactiveMBean()
	{
		return null;
	}
}
