/*
 * HA-JDBC: High-Availability JDBC
 * Copyright 2004-2009 Paul Ferraro
 * 
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 * 
 * You should have received a copy of the GNU Lesser General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package net.sf.hajdbc.sql;

import java.sql.Connection;
import java.sql.SQLException;

import javax.sql.DataSource;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlType;

import net.sf.hajdbc.codec.Codec;
import net.sf.hajdbc.management.Description;
import net.sf.hajdbc.management.MBean;


/**
 * A database described by a {@link DataSource}.
 * @author  Paul Ferraro
 */
@MBean
@Description("Database accessed via a DataSource")
@XmlType(name = "database")
@XmlAccessorType(XmlAccessType.FIELD)
public class DataSourceDatabase extends CommonDataSourceDatabase<DataSource>
{
	/**
	 * @param dataSource A DataSource
	 * @return a database connection
	 * @throws SQLException if a database connection could not be made
	 * @see net.sf.hajdbc.Database#connect(Object)
	 */
	@Override
	public Connection connect(DataSource dataSource, Codec codec) throws SQLException
	{
		return this.requiresAuthentication() ? dataSource.getConnection(this.getUser(), codec.decode(this.getPassword())) : dataSource.getConnection();
	}

	@Override
	protected Class<DataSource> getTargetClass()
	{
		return DataSource.class;
	}
}
