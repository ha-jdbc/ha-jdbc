/*
 * HA-JDBC: High-Availability JDBC
 * Copyright (C) 2012  Paul Ferraro
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
package net.sf.hajdbc.sql.pool;

import java.sql.Connection;
import java.sql.SQLException;

import javax.sql.ConnectionPoolDataSource;
import javax.sql.PooledConnection;
import javax.xml.bind.annotation.XmlType;

import net.sf.hajdbc.Credentials;
import net.sf.hajdbc.codec.Decoder;
import net.sf.hajdbc.management.Description;
import net.sf.hajdbc.management.MBean;
import net.sf.hajdbc.sql.AbstractDatabase;

/**
 * A database described by a {@link ConnectionPoolDataSource}.
 * @author Paul Ferraro
 */
@MBean
@Description("Database accessed via a server-side ConnectionPoolDataSource")
@XmlType(name = "database")
public class ConnectionPoolDataSourceDatabase extends AbstractDatabase<ConnectionPoolDataSource>
{
	public ConnectionPoolDataSourceDatabase(String id, ConnectionPoolDataSource dataSource, Credentials credentials, int weight, boolean local)
	{
		super(id, dataSource, credentials, weight, local);
	}

	@Override
	public Connection connect(Decoder decoder) throws SQLException
	{
		ConnectionPoolDataSource dataSource = this.getConnectionSource();
		Credentials credentials = this.getCredentials();
		PooledConnection connection = (credentials != null) ? dataSource.getPooledConnection(credentials.getUser(), credentials.decodePassword(decoder)) : dataSource.getPooledConnection();
		return connection.getConnection();
	}
}
