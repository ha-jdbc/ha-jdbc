/*
 * HA-JDBC: High-Availability JDBC
 * Copyright (C) 2005 Paul Ferraro
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
import java.io.StringWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Hashtable;

import javax.naming.Context;
import javax.naming.Name;
import javax.naming.spi.ObjectFactory;
import javax.sql.DataSource;

/**
 * Mock data source factory that creates a mock data source that references a mock connection.
 * 
 * @author  Paul Ferraro
 * @since   1.1
 */
public class MockDataSourceFactory implements ObjectFactory
{
	/**
	 * @see javax.naming.spi.ObjectFactory#getObjectInstance(java.lang.Object, javax.naming.Name, javax.naming.Context, java.util.Hashtable)
	 */
	public Object getObjectInstance(Object arg0, Name arg1, Context arg2, Hashtable arg3) throws Exception
	{
		return new MockDataSource();
	}
	
	private class MockDataSource implements DataSource
	{
		/**
		 * @see javax.sql.DataSource#getConnection()
		 */
		public Connection getConnection() throws SQLException
		{
			return new MockConnection();
		}

		/**
		 * @see javax.sql.DataSource#getConnection(java.lang.String, java.lang.String)
		 */
		public Connection getConnection(String arg0, String arg1) throws SQLException
		{
			return this.getConnection();
		}

		/**
		 * @see javax.sql.DataSource#getLogWriter()
		 */
		public PrintWriter getLogWriter() throws SQLException
		{
			return new PrintWriter(new StringWriter());
		}

		/**
		 * @see javax.sql.DataSource#setLogWriter(java.io.PrintWriter)
		 */
		public void setLogWriter(PrintWriter writer) throws SQLException
		{
		}

		/**
		 * @see javax.sql.DataSource#setLoginTimeout(int)
		 */
		public void setLoginTimeout(int timeout) throws SQLException
		{
		}

		/**
		 * @see javax.sql.DataSource#getLoginTimeout()
		 */
		public int getLoginTimeout() throws SQLException
		{
			return 0;
		}
	}
}
