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
	 * @see net.sf.hajdbc.Database#createConnectionSource()
	 */
	@Override
	public DataSource createConnectionSource()
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
