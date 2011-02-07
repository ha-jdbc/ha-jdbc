/*
 * HA-JDBC: High-Availablity JDBC
 * Copyright 2004-Apr 26, 2010 Paul Ferraro
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

import java.lang.reflect.InvocationHandler;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.Map;

import net.sf.hajdbc.Database;
import net.sf.hajdbc.invocation.Invoker;

/**
 * @author paul
 *
 */
public class DatabaseMetaDataInvocationHandlerFactory<Z, D extends Database<Z>> implements InvocationHandlerFactory<Z, D, Connection, DatabaseMetaData, SQLException>
{
	@Override
	public InvocationHandler createInvocationHandler(Connection connection, SQLProxy<Z, D, Connection, SQLException> proxy, Invoker<Z, D, Connection, DatabaseMetaData, SQLException> invoker, Map<D, DatabaseMetaData> objects) throws SQLException
	{
		return new DatabaseMetaDataInvocationHandler<Z, D>(connection, proxy, invoker, objects);
	}

	@Override
	public Class<DatabaseMetaData> getTargetClass()
	{
		return DatabaseMetaData.class;
	}
}
