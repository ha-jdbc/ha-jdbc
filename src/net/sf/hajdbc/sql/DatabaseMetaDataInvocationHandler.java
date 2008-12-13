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
package net.sf.hajdbc.sql;

import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.Map;
import java.util.Set;

import net.sf.hajdbc.Database;
import net.sf.hajdbc.util.reflect.Methods;

/**
 * @author Paul Ferraro
 *
 */
public class DatabaseMetaDataInvocationHandler<D> extends AbstractChildInvocationHandler<D, Connection, DatabaseMetaData>
{
	private static final Set<Method> databaseReadMethodSet = Methods.findMethods(DatabaseMetaData.class, "getAttributes", "getBestRowIdentifier", "getCatalogs", "getColumnPrivileges", "getColumns", "getCrossReference", "getExportedKeys", "getFunctionColumns", "getFunctions", "getImportedKeys", "getIndexInfo", "getPrimaryKeys", "getProcedureColumns", "getProcedures", "getSchemas", "getSuperTables", "getSuperTypes", "getTablePrivileges", "getTables", "getUDTs", "getVersionColumns");
	private static final Method getConnectionMethod = Methods.getMethod(DatabaseMetaData.class, "getConnection");
	
	/**
	 * @param parent
	 * @param proxy
	 * @param invoker
	 * @param objectMap
	 * @throws Exception
	 */
	public DatabaseMetaDataInvocationHandler(Connection parent, SQLProxy<D, Connection> proxy, Invoker<D, Connection, DatabaseMetaData> invoker, Map<Database<D>, DatabaseMetaData> objectMap) throws Exception
	{
		super(parent, proxy, invoker, DatabaseMetaData.class, objectMap);
	}

	@Override
	protected InvocationStrategy<D, DatabaseMetaData, ?> getInvocationStrategy(DatabaseMetaData object, Method method, Object[] parameters) throws Exception
	{
		if (databaseReadMethodSet.contains(method))
		{
			return new DatabaseReadInvocationStrategy<D, DatabaseMetaData, Object>();
		}
		
		if (method.equals(getConnectionMethod))
		{
			return new InvocationStrategy<D, DatabaseMetaData, Connection>()
			{
				public Connection invoke(SQLProxy<D, DatabaseMetaData> proxy, Invoker<D, DatabaseMetaData, Connection> invoker) throws Exception
				{
					return DatabaseMetaDataInvocationHandler.this.getParent();
				}
			};
		}
		
		return new DriverReadInvocationStrategy<D, DatabaseMetaData, Object>();
	}

	@Override
	protected void close(Connection parent, DatabaseMetaData object) throws SQLException
	{
		// Nothing to close
	}
}
