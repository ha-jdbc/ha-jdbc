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
import java.sql.DatabaseMetaData;
import java.sql.SQLException;

import net.sf.hajdbc.Database;
import net.sf.hajdbc.util.reflect.ProxyFactory;

/**
 * @author Paul Ferraro
 *
 */
public class DatabaseMetaDataInvocationStrategy<Z, D extends Database<Z>> extends DatabaseReadInvocationStrategy<Z, D, Connection, DatabaseMetaData, SQLException>
{
	private final Connection connection;
	
	public DatabaseMetaDataInvocationStrategy(Connection connection)
	{
		this.connection = connection;
	}
	
	@Override
	public DatabaseMetaData invoke(SQLProxy<Z, D, Connection, SQLException> proxy, Invoker<Z, D, Connection, DatabaseMetaData, SQLException> invoker) throws SQLException
	{
		return ProxyFactory.createProxy(DatabaseMetaData.class, new DatabaseMetaDataInvocationHandler<Z, D>(this.connection, proxy, invoker, this.invokeAll(proxy, invoker)));
	}
}
