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
package net.sf.hajdbc.sql.xa;

import java.sql.Connection;
import java.sql.SQLException;

import javax.sql.XAConnection;
import javax.sql.XADataSource;
import javax.xml.bind.annotation.XmlType;

import net.sf.hajdbc.Credentials;
import net.sf.hajdbc.codec.Decoder;
import net.sf.hajdbc.management.Description;
import net.sf.hajdbc.management.MBean;
import net.sf.hajdbc.sql.AbstractDatabase;

/**
 * A database described by an {@link XADataSource}.
 * @author Paul Ferraro
 */
@MBean
@Description("Database accessed via a server-side XADataSource")
@XmlType(name = "database")
public class XADataSourceDatabase extends AbstractDatabase<XADataSource>
{
	private boolean force2PC = false;
	
	public XADataSourceDatabase(String id, XADataSource dataSource, Credentials credentials, int weight, boolean local, boolean force2PC)
	{
		super(id, dataSource, credentials, weight, local);
		this.force2PC = force2PC;
	}

	@Override
	public Connection connect(Decoder decoder) throws SQLException
	{
		XADataSource dataSource = this.getConnectionSource();
		Credentials credentials = this.getCredentials();
		XAConnection connection = (credentials != null) ? dataSource.getXAConnection(credentials.getUser(), credentials.decodePassword(decoder)) : dataSource.getXAConnection();
		return connection.getConnection();
	}

	public boolean isForce2PC()
	{
		return this.force2PC;
	}
}
