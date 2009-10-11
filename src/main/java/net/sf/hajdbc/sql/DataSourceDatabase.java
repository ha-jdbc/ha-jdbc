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

import javax.sql.DataSource;

import net.sf.hajdbc.management.Managed;


/**
 * A database described by a {@link DataSource}.
 * @author  Paul Ferraro
 */
@Managed(description = "Database accessed via a DataSource")
public class DataSourceDatabase extends CommonDataSourceDatabase<DataSource>
{
	/**
	 * @param dataSource A DataSource
	 * @return a database connection
	 * @throws SQLException if a database connection could not be made
	 * @see net.sf.hajdbc.Database#connect(Object)
	 */
	@Override
	public Connection connect(DataSource dataSource) throws SQLException
	{
		return this.requiresAuthentication() ? dataSource.getConnection(this.getUser(), this.getPassword()) : dataSource.getConnection();
	}

	@Override
	protected Class<DataSource> getTargetClass()
	{
		return DataSource.class;
	}
}
