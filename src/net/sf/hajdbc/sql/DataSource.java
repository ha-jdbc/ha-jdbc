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
	public static final String NAME = "name";
	
	private String name;
	private ConnectionFactory<javax.sql.DataSource> connectionFactory;

	/**
	 * Returns the name of this DataSource
	 * @return the name of this DataSource
	 */
	public String getName()
	{
		return this.name;
	}
	
	/**
	 * Sets the name of this DataSource
	 * @param name the name of this DataSource
	 * @throws java.sql.SQLException
	 */
	public void setName(String name) throws java.sql.SQLException
	{
		this.name = name;
	}
	
	/**
	 * @see javax.naming.Referenceable#getReference()
	 */
	public final Reference getReference()
	{
        Reference ref = new Reference(this.getClass().getName(), DataSourceFactory.class.getName(), null);
        
        ref.add(new StringRefAddr(NAME, this.getName()));
        
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
		Operation<javax.sql.DataSource, Integer> operation = new Operation<javax.sql.DataSource, Integer>()
		{
			public Integer execute(Database database, javax.sql.DataSource dataSource) throws SQLException
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
		Operation<javax.sql.DataSource, Void> operation = new Operation<javax.sql.DataSource, Void>()
		{
			public Void execute(Database database, javax.sql.DataSource dataSource) throws SQLException
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
		Operation<javax.sql.DataSource, PrintWriter> operation = new Operation<javax.sql.DataSource, PrintWriter>()
		{
			public PrintWriter execute(Database database, javax.sql.DataSource dataSource) throws SQLException
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
		Operation<javax.sql.DataSource, Void> operation = new Operation<javax.sql.DataSource, Void>()
		{
			public Void execute(Database database, javax.sql.DataSource dataSource) throws SQLException
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
		Operation<javax.sql.DataSource, java.sql.Connection> operation = new Operation<javax.sql.DataSource, java.sql.Connection>()
		{
			public java.sql.Connection execute(Database database, javax.sql.DataSource dataSource) throws SQLException
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
		Operation<javax.sql.DataSource, java.sql.Connection> operation = new Operation<javax.sql.DataSource, java.sql.Connection>()
		{
			public java.sql.Connection execute(Database database, javax.sql.DataSource dataSource) throws SQLException
			{
				return dataSource.getConnection(user, password);
			}
		};
		
		return new Connection<javax.sql.DataSource>(this.connectionFactory, operation, new FileSupportImpl());
	}
}
