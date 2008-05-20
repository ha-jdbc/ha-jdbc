/*
 * HA-JDBC: High-Availability JDBC
 * Copyright (c) 2004-2008 Paul Ferraro
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
package net.sf.hajdbc.sql.xa;

import java.sql.SQLException;

import javax.naming.NamingException;
import javax.naming.Reference;
import javax.sql.XAConnection;

import net.sf.hajdbc.sql.CommonDataSourceProxy;

/**
 * @author Paul Ferraro
 */
public class XADataSource extends CommonDataSourceProxy<javax.sql.XADataSource> implements javax.sql.XADataSource
{
	/**
	 * Constructs a new XADataSource
	 */
	public XADataSource()
	{
		super(new XADataSourceFactory());
	}

	/**
	 * @see javax.sql.XADataSource#getXAConnection()
	 */
	@Override
	public XAConnection getXAConnection() throws SQLException
	{
		return this.getProxy().getXAConnection();
	}

	/**
	 * @see javax.sql.XADataSource#getXAConnection(java.lang.String, java.lang.String)
	 */
	@Override
	public XAConnection getXAConnection(String user, String password) throws SQLException
	{
		return this.getProxy().getXAConnection(user, password);
	}

	/**
	 * @see javax.naming.Referenceable#getReference()
	 */
	@Override
	public Reference getReference() throws NamingException
	{
		return new XADataSourceReference(this.getCluster(), this.getConfig());
	}
}
