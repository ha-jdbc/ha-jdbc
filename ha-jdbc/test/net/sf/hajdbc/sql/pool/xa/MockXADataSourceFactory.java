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
package net.sf.hajdbc.sql.pool.xa;

import java.sql.SQLException;
import java.util.Hashtable;

import javax.naming.Context;
import javax.naming.Name;
import javax.sql.XAConnection;
import javax.sql.XADataSource;

import net.sf.hajdbc.sql.pool.MockConnectionPoolDataSourceFactory;

public class MockXADataSourceFactory extends MockConnectionPoolDataSourceFactory
{
	/**
	 * @see net.sf.hajdbc.sql.pool.MockConnectionPoolDataSourceFactory#getObjectInstance(java.lang.Object, javax.naming.Name, javax.naming.Context, java.util.Hashtable)
	 */
	public Object getObjectInstance(Object arg0, Name arg1, Context arg2, Hashtable arg3) throws Exception
	{
		return new MockXADataSource();
	}
	
	public class MockXADataSource extends MockConnectionPoolDataSource implements XADataSource
	{
		public XAConnection getXAConnection() throws SQLException
		{
			return new MockXAConnection();
		}

		public XAConnection getXAConnection(String arg0, String arg1) throws SQLException
		{
			return this.getXAConnection();
		}		
	}
}
