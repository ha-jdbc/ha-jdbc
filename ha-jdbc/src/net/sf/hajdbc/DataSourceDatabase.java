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
package net.sf.hajdbc;

import java.sql.Connection;

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.sql.DataSource;

/**
 * @author  Paul Ferraro
 * @version $Revision$
 * @since   1.0
 */
public class DataSourceDatabase extends AbstractDatabase
{
	protected String name;
	
	/**
	 * @return the DataSource JNDI name
	 */
	public String getName()
	{
		return this.name;
	}
	
	/**
	 * @param name
	 */
	public void setName(String name)
	{
		this.name = name;
	}

	/**
	 * @see net.sf.hajdbc.Database#getId()
	 */
	public String getId()
	{
		return this.name;
	}
	
	/**
	 * @see net.sf.hajdbc.Database#connect(java.lang.Object)
	 */
	public Connection connect(Object connectionFactory) throws java.sql.SQLException
	{
		DataSource dataSource = (DataSource) connectionFactory;
		
		return (this.user != null) ? dataSource.getConnection(this.user, this.password) : dataSource.getConnection();
	}

	/**
	 * @see net.sf.hajdbc.Database#getDatabaseConnector()
	 */
	public Object getConnectionFactory() throws java.sql.SQLException
	{
		try
		{
			Context context = new InitialContext();
	
			Object object = context.lookup(this.name);
	
			if (!this.getDataSourceClass().isInstance(object))
			{
				throw new SQLException(this.name + " does not implement " + this.getDataSourceClass().getName());
			}
			
			return object;
		}
		catch (NamingException e)
		{
			throw new SQLException("Failed to perform naming lookup of " + this.name, e);
		}
	}
	
	protected Class getDataSourceClass()
	{
		return DataSource.class;
	}
}
