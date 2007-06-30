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
import javax.management.NotCompliantMBeanException;
import javax.management.StandardMBean;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.sql.DataSource;

import net.sf.hajdbc.Messages;

/**
 * @author  Paul Ferraro
 * @version $Revision$
 * @since   1.0
 */
public class DataSourceDatabase extends AbstractDatabase<DataSource> implements InactiveDataSourceDatabaseMBean
{
	String name;
	
	/**
	 * @see net.sf.hajdbc.sql.ActiveDataSourceDatabaseMBean#getName()
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
		this.checkDirty(this.name, name);
		this.name = name;
	}
	
	/**
	 * @param dataSource A DataSource
	 * @return a database connection
	 * @throws SQLException if a database connection could not be made
	 * @see net.sf.hajdbc.Database#connect(Object)
	 */
	public Connection connect(DataSource dataSource) throws SQLException
	{
		return (this.user != null) ? dataSource.getConnection(this.user, this.password) : dataSource.getConnection();
	}

	/**
	 * @see net.sf.hajdbc.Database#createConnectionFactory()
	 */
	public DataSource createConnectionFactory()
	{
		try
		{
			Context context = new InitialContext(this.properties);
	
			return (DataSource) context.lookup(this.name);
		}
		catch (ClassCastException e)
		{
			throw new IllegalArgumentException(e.toString(), e);
		}
		catch (NamingException e)
		{
			throw new IllegalArgumentException(Messages.getMessage(Messages.JNDI_LOOKUP_FAILED, this.name), e);
		}
	}

	/**
	 * @throws NotCompliantMBeanException 
	 * @see net.sf.hajdbc.Database#getActiveMBean()
	 */
	public DynamicMBean getActiveMBean()
	{
		try
		{
			return new StandardMBean(this, ActiveDataSourceDatabaseMBean.class);
		}
		catch (NotCompliantMBeanException e)
		{
			throw new IllegalStateException(e);
		}
	}

	/**
	 * @throws NotCompliantMBeanException 
	 * @see net.sf.hajdbc.Database#getInactiveMBean()
	 */
	public DynamicMBean getInactiveMBean()
	{
		try
		{
			return new StandardMBean(this, InactiveDataSourceDatabaseMBean.class);
		}
		catch (NotCompliantMBeanException e)
		{
			throw new IllegalStateException(e);
		}
	}
}
