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
package net.sf.hajdbc.sql;

import java.io.PrintWriter;
import java.sql.SQLException;


/**
 * @author Paul Ferraro
 * @version $Revision$
 */
public class DataSource extends AbstractDataSource implements javax.sql.DataSource
{
	/**
	 * @see javax.sql.DataSource#getLoginTimeout()
	 */
	public int getLoginTimeout() throws SQLException
	{
		DataSourceOperation operation = new DataSourceOperation()
		{
			public Object execute(javax.sql.DataSource dataSource) throws SQLException
			{
				return new Integer(dataSource.getLoginTimeout());
			}
		};
		
		return ((Integer) this.connectionFactory.executeReadFromDriver(operation)).intValue();
	}

	/**
	 * @see javax.sql.DataSource#setLoginTimeout(int)
	 */
	public void setLoginTimeout(final int seconds) throws SQLException
	{
		DataSourceOperation operation = new DataSourceOperation()
		{
			public Object execute(javax.sql.DataSource dataSource) throws SQLException
			{
				dataSource.setLoginTimeout(seconds);
				
				return null;
			}
		};
		
		this.connectionFactory.executeWriteToDriver(operation);
	}

	/**
	 * @see javax.sql.DataSource#getLogWriter()
	 */
	public PrintWriter getLogWriter() throws SQLException
	{
		DataSourceOperation operation = new DataSourceOperation()
		{
			public Object execute(javax.sql.DataSource dataSource) throws SQLException
			{
				return dataSource.getLogWriter();
			}
		};
		
		return (PrintWriter) this.connectionFactory.executeReadFromDriver(operation);
	}

	/**
	 * @see javax.sql.DataSource#setLogWriter(java.io.PrintWriter)
	 */
	public void setLogWriter(final PrintWriter writer) throws SQLException
	{
		DataSourceOperation operation = new DataSourceOperation()
		{
			public Object execute(javax.sql.DataSource dataSource) throws SQLException
			{
				dataSource.setLogWriter(writer);
				
				return null;
			}
		};
		
		this.connectionFactory.executeWriteToDriver(operation);
	}

	/**
	 * @see javax.sql.DataSource#getConnection()
	 */
	public java.sql.Connection getConnection() throws SQLException
	{
		DataSourceOperation operation = new DataSourceOperation()
		{
			public Object execute(javax.sql.DataSource dataSource) throws SQLException
			{
				return dataSource.getConnection();
			}
		};
		
		return new Connection(this.connectionFactory, operation, new FileSupportImpl());
	}

	/**
	 * @see javax.sql.DataSource#getConnection(java.lang.String, java.lang.String)
	 */
	public java.sql.Connection getConnection(final String user, final String password) throws SQLException
	{
		DataSourceOperation operation = new DataSourceOperation()
		{
			public Object execute(javax.sql.DataSource dataSource) throws SQLException
			{
				return dataSource.getConnection(user, password);
			}
		};
		
		return new Connection(this.connectionFactory, operation, new FileSupportImpl());
	}

	/**
	 * @see net.sf.hajdbc.sql.AbstractDataSource#getObjectFactoryClass()
	 */
	protected Class getObjectFactoryClass()
	{
		return DataSourceFactory.class;
	}
}
