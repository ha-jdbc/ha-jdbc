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

import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.Map;
import java.util.Set;

import net.sf.hajdbc.Database;
import net.sf.hajdbc.invocation.InvocationStrategy;
import net.sf.hajdbc.invocation.InvocationStrategyEnum;
import net.sf.hajdbc.invocation.Invoker;
import net.sf.hajdbc.util.reflect.Methods;

/**
 * @author Paul Ferraro
 *
 */
public class DatabaseMetaDataInvocationHandler<Z, D extends Database<Z>> extends ChildInvocationHandler<Z, D, Connection, DatabaseMetaData, SQLException>
{
	private static final Set<Method> databaseReadMethodSet = Methods.findMethods(DatabaseMetaData.class, "getAttributes", "getBestRowIdentifier", "getCatalogs", "getColumnPrivileges", "getColumns", "getCrossReference", "getExportedKeys", "getFunctionColumns", "getFunctions", "getImportedKeys", "getIndexInfo", "getPrimaryKeys", "getProcedureColumns", "getProcedures", "getPseudoColumns", "getSchemas", "getSuperTables", "getSuperTypes", "getTablePrivileges", "getTables", "getUDTs", "getVersionColumns");
	private static final Method getConnectionMethod = Methods.getMethod(DatabaseMetaData.class, "getConnection");
	
	/**
	 * @param parent
	 * @param proxy
	 * @param invoker
	 * @param objectMap
	 * @throws Exception
	 */
	public DatabaseMetaDataInvocationHandler(Connection parent, SQLProxy<Z, D, Connection, SQLException> proxy, Invoker<Z, D, Connection, DatabaseMetaData, SQLException> invoker, Map<D, DatabaseMetaData> objectMap)
	{
		super(parent, proxy, invoker, DatabaseMetaData.class, SQLException.class, objectMap);
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.sql.AbstractChildInvocationHandler#getParentMethod()
	 */
	@Override
	protected Method getParentMethod()
	{
		return getConnectionMethod;
	}

	@Override
	protected InvocationStrategy getInvocationStrategy(DatabaseMetaData metaData, Method method, Object[] parameters) throws SQLException
	{
		if (databaseReadMethodSet.contains(method))
		{
			return InvocationStrategyEnum.INVOKE_ON_NEXT;
		}
		
		return InvocationStrategyEnum.INVOKE_ON_ANY;
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.sql.AbstractChildInvocationHandler#close(java.lang.Object, java.lang.Object)
	 */
	@Override
	protected void close(Connection parent, DatabaseMetaData object) throws SQLException
	{
		// Nothing to close
	}
}
