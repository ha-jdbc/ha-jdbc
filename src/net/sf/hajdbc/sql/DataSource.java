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
package net.sf.hajdbc.sql;

import java.io.PrintWriter;
import java.sql.SQLException;

import javax.naming.Reference;
import javax.naming.Referenceable;
import javax.naming.StringRefAddr;

import net.sf.hajdbc.Database;
import net.sf.hajdbc.Operation;


/**
 * @author Paul Ferraro
 * @version $Revision$
 */
public class DataSource implements javax.sql.DataSource, Referenceable
{
	/**	Property that identifies this data source */
	public static final String DATABASE_CLUSTER = "cluster";
	
	private String cluster;
	private ConnectionFactory<javax.sql.DataSource> connectionFactory;

	/**
	 * Returns the identifier of the database cluster represented by this DataSource
	 * @return a database cluster identifier
	 */
	public String getCluster()
	{
		return this.cluster;
	}
	
	/**
	 * Sets the identifier of the database cluster represented by this DataSource
	 * @param cluster a database cluster identifier
	 */
	public void setCluster(String cluster)
	{
		this.cluster = cluster;
	}
	
	/**
	 * @see javax.naming.Referenceable#getReference()
	 */
	public final Reference getReference()
	{
        Reference ref = new Reference(this.getClass().getName(), DataSourceFactory.class.getName(), null);
        
        ref.add(new StringRefAddr(DATABASE_CLUSTER, this.cluster));
        
        return ref;
	}
	
	/**
	 * Set the connection factory for this datasource.
	 * @param connectionFactory a factory for creating database connections
	 */
	public void setConnectionFactory(ConnectionFactory<javax.sql.DataSource> connectionFactory)
	{
		this.connectionFactory = connectionFactory;
	}
	
	/**
	 * @see javax.sql.DataSource#getLoginTimeout()
	 */
	public int getLoginTimeout() throws SQLException
	{
		DataSourceOperation<Integer> operation = new DataSourceOperation<Integer>()
		{
			public Integer execute(Database<javax.sql.DataSource> database, javax.sql.DataSource dataSource) throws SQLException
			{
				return dataSource.getLoginTimeout();
			}
		};
		
		return this.connectionFactory.executeReadFromDriver(operation);
	}

	/**
	 * @see javax.sql.DataSource#setLoginTimeout(int)
	 */
	public void setLoginTimeout(final int seconds) throws SQLException
	{
		DataSourceOperation<Void> operation = new DataSourceOperation<Void>()
		{
			public Void execute(Database<javax.sql.DataSource> database, javax.sql.DataSource dataSource) throws SQLException
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
		DataSourceOperation<PrintWriter> operation = new DataSourceOperation<PrintWriter>()
		{
			public PrintWriter execute(Database<javax.sql.DataSource> database, javax.sql.DataSource dataSource) throws SQLException
			{
				return dataSource.getLogWriter();
			}
		};
		
		return this.connectionFactory.executeReadFromDriver(operation);
	}

	/**
	 * @see javax.sql.DataSource#setLogWriter(java.io.PrintWriter)
	 */
	public void setLogWriter(final PrintWriter writer) throws SQLException
	{
		DataSourceOperation<Void> operation = new DataSourceOperation<Void>()
		{
			public Void execute(Database<javax.sql.DataSource> database, javax.sql.DataSource dataSource) throws SQLException
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
		DataSourceOperation<java.sql.Connection> operation = new DataSourceOperation<java.sql.Connection>()
		{
			public java.sql.Connection execute(Database<javax.sql.DataSource> database, javax.sql.DataSource dataSource) throws SQLException
			{
				return dataSource.getConnection();
			}
		};
		
		return new Connection<javax.sql.DataSource>(this.connectionFactory, operation, new FileSupportImpl());
	}

	/**
	 * @see javax.sql.DataSource#getConnection(java.lang.String, java.lang.String)
	 */
	public java.sql.Connection getConnection(final String user, final String password) throws SQLException
	{
		DataSourceOperation<java.sql.Connection> operation = new DataSourceOperation<java.sql.Connection>()
		{
			public java.sql.Connection execute(Database<javax.sql.DataSource> database, javax.sql.DataSource dataSource) throws SQLException
			{
				return dataSource.getConnection(user, password);
			}
		};
		
		return new Connection<javax.sql.DataSource>(this.connectionFactory, operation, new FileSupportImpl());
	}
	
	private interface DataSourceOperation<R> extends Operation<javax.sql.DataSource, javax.sql.DataSource, R>
	{
		
	}
}
