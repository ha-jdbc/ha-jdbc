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
package net.sf.hajdbc.sql.pool.xa;

import java.io.PrintWriter;
import java.sql.SQLException;

import net.sf.hajdbc.Database;
import net.sf.hajdbc.DatabaseCluster;
import net.sf.hajdbc.Operation;
import net.sf.hajdbc.sql.AbstractDataSource;
import net.sf.hajdbc.sql.AbstractDataSourceFactory;
import net.sf.hajdbc.sql.ConnectionFactory;

/**
 * @author  Paul Ferraro
 * @version $Revision$
 * @since   1.0
 */
public class XADataSource extends AbstractDataSource implements javax.sql.XADataSource
{
	private ConnectionFactory<javax.sql.XADataSource> connectionFactory;
	
	/**
	 * @see net.sf.hajdbc.sql.AbstractDataSource#setDatabaseCluster(net.sf.hajdbc.DatabaseCluster)
	 */
	public void setDatabaseCluster(DatabaseCluster databaseCluster)
	{
		this.connectionFactory = new ConnectionFactory<javax.sql.XADataSource>(databaseCluster, javax.sql.XADataSource.class);
	}
	
	/**
	 * @see javax.sql.XADataSource#getXAConnection()
	 */
	public javax.sql.XAConnection getXAConnection() throws SQLException
	{
		Operation<javax.sql.XADataSource, javax.sql.XAConnection> operation = new Operation<javax.sql.XADataSource, javax.sql.XAConnection>()
		{
			public javax.sql.XAConnection execute(Database database, javax.sql.XADataSource dataSource) throws SQLException
			{
				return dataSource.getXAConnection();
			}
		};
		
		return new XAConnection(this.connectionFactory, operation);
	}

	/**
	 * @see javax.sql.XADataSource#getXAConnection(java.lang.String, java.lang.String)
	 */
	public javax.sql.XAConnection getXAConnection(final String user, final String password) throws SQLException
	{
		Operation<javax.sql.XADataSource, javax.sql.XAConnection> operation = new Operation<javax.sql.XADataSource, javax.sql.XAConnection>()
		{
			public javax.sql.XAConnection execute(Database database, javax.sql.XADataSource dataSource) throws SQLException
			{
				return dataSource.getXAConnection(user, password);
			}
		};
		
		return new XAConnection(this.connectionFactory, operation);
	}

	/**
	 * @see javax.sql.XADataSource#getLoginTimeout()
	 */
	public int getLoginTimeout() throws SQLException
	{
		Operation<javax.sql.XADataSource, Integer> operation = new Operation<javax.sql.XADataSource, Integer>()
		{
			public Integer execute(Database database, javax.sql.XADataSource dataSource) throws SQLException
			{
				return dataSource.getLoginTimeout();
			}
		};
		
		return this.connectionFactory.executeReadFromDriver(operation);
	}
	
	/**
	 * @see javax.sql.XADataSource#setLoginTimeout(int)
	 */
	public void setLoginTimeout(final int seconds) throws SQLException
	{
		Operation<javax.sql.XADataSource, Integer> operation = new Operation<javax.sql.XADataSource, Integer>()
		{
			public Integer execute(Database database, javax.sql.XADataSource dataSource) throws SQLException
			{
				dataSource.setLoginTimeout(seconds);
				
				return null;
			}
		};
		
		this.connectionFactory.executeWriteToDriver(operation);
	}

	/**
	 * @see javax.sql.XADataSource#getLogWriter()
	 */
	public PrintWriter getLogWriter() throws SQLException
	{
		Operation<javax.sql.XADataSource, PrintWriter> operation = new Operation<javax.sql.XADataSource, PrintWriter>()
		{
			public PrintWriter execute(Database database, javax.sql.XADataSource dataSource) throws SQLException
			{
				return dataSource.getLogWriter();
			}
		};
		
		return this.connectionFactory.executeReadFromDriver(operation);
	}

	/**
	 * @see javax.sql.XADataSource#setLogWriter(java.io.PrintWriter)
	 */
	public void setLogWriter(final PrintWriter writer) throws SQLException
	{
		Operation<javax.sql.XADataSource, Integer> operation = new Operation<javax.sql.XADataSource, Integer>()
		{
			public Integer execute(Database database, javax.sql.XADataSource dataSource) throws SQLException
			{
				dataSource.setLogWriter(writer);
				
				return null;
			}
		};
		
		this.connectionFactory.executeWriteToDriver(operation);
	}

	/**
	 * @see net.sf.hajdbc.sql.AbstractDataSource#getObjectFactoryClass()
	 */
	protected Class<? extends AbstractDataSourceFactory> getObjectFactoryClass()
	{
		return XADataSourceFactory.class;
	}
}
