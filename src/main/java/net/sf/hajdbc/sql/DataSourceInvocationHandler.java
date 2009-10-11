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
import java.sql.SQLException;
import java.util.Set;

import javax.sql.DataSource;

import net.sf.hajdbc.DatabaseCluster;
import net.sf.hajdbc.util.reflect.Methods;

/**
 * @author Paul Ferraro
 *
 */
@SuppressWarnings("nls")
public class DataSourceInvocationHandler extends CommonDataSourceInvocationHandler<DataSource, DataSourceDatabase>
{
	private static final Set<Method> getConnectionMethods = Methods.findMethods(DataSource.class, "getConnection");
	
	/**
	 * @param databaseCluster
	 */
	public DataSourceInvocationHandler(DatabaseCluster<DataSource, DataSourceDatabase> databaseCluster)
	{
		super(databaseCluster, DataSource.class);
	}

	/**
	 * @see net.sf.hajdbc.sql.AbstractChildInvocationHandler#getInvocationStrategy(java.lang.Object, java.lang.reflect.Method, java.lang.Object[])
	 */
	@Override
	protected InvocationStrategy<DataSource, DataSourceDatabase, DataSource, ?, SQLException> getInvocationStrategy(DataSource dataSource, Method method, Object[] parameters) throws SQLException
	{
		if (getConnectionMethods.contains(method))
		{
			TransactionContext<DataSource, DataSourceDatabase> context = new LocalTransactionContext<DataSource, DataSourceDatabase>(this.cluster);
			
			return new ConnectionInvocationStrategy<DataSource, DataSourceDatabase, DataSource>(this.cluster, dataSource, context);
		}
		
		return super.getInvocationStrategy(dataSource, method, parameters);
	}
}
