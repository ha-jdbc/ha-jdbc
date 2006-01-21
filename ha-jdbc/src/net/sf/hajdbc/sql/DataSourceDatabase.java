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

import java.sql.Connection;

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.sql.DataSource;

import net.sf.hajdbc.Messages;
import net.sf.hajdbc.SQLException;

/**
 * @author  Paul Ferraro
 * @version $Revision$
 * @since   1.0
 */
public class DataSourceDatabase extends AbstractDatabase<DataSource>
{
	protected String name;
	
	/**
	 * Return the JNDI name of this DataSource
	 * @return a JNDI name
	 */
	public String getName()
	{
		return this.name;
	}
	
	/**
	 * Sets the JNDI name of this DataSource
	 * @param name a JNDI name
	 */
	public void setName(String name)
	{
		this.name = name;
	}
	
	/**
	 * @see net.sf.hajdbc.Database#connect(T)
	 */
	public Connection connect(DataSource dataSource) throws java.sql.SQLException
	{
		return (this.user != null) ? dataSource.getConnection(this.user, this.password) : dataSource.getConnection();
	}

	/**
	 * @see net.sf.hajdbc.Database#createConnectionFactory()
	 */
	public DataSource createConnectionFactory() throws java.sql.SQLException
	{
		try
		{
			Context context = new InitialContext(this.properties);
	
			Object object = context.lookup(this.name);
			
			return DataSource.class.cast(object);
		}
		catch (ClassCastException e)
		{
			throw new SQLException(Messages.getMessage(Messages.NOT_INSTANCE_OF, this.name, DataSource.class.getName()), e);
		}
		catch (NamingException e)
		{
			throw new SQLException(Messages.getMessage(Messages.JNDI_LOOKUP_FAILED, this.name), e);
		}
	}
}
