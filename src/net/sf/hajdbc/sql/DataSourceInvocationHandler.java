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

import java.lang.reflect.Method;
import java.util.Set;

import javax.sql.DataSource;

import net.sf.hajdbc.DatabaseCluster;
import net.sf.hajdbc.util.reflect.Methods;

/**
 * @author Paul Ferraro
 *
 */
@SuppressWarnings("nls")
public class DataSourceInvocationHandler extends CommonDataSourceInvocationHandler<DataSource>
{
	private static final Set<Method> getConnectionMethods = Methods.findMethods(DataSource.class, "getConnection");
	
	/**
	 * @param databaseCluster
	 */
	public DataSourceInvocationHandler(DatabaseCluster<DataSource> databaseCluster)
	{
		super(databaseCluster, DataSource.class);
	}

	/**
	 * @see net.sf.hajdbc.sql.AbstractChildInvocationHandler#getInvocationStrategy(java.lang.Object, java.lang.reflect.Method, java.lang.Object[])
	 */
	@Override
	protected InvocationStrategy<DataSource, DataSource, ?> getInvocationStrategy(DataSource dataSource, Method method, Object[] parameters) throws Exception
	{
		if (getConnectionMethods.contains(method))
		{
			TransactionContext<DataSource> context = new LocalTransactionContext<DataSource>(this.cluster);
			
			return new ConnectionInvocationStrategy<DataSource, DataSource>(this.cluster, dataSource, context);
		}
		
		return super.getInvocationStrategy(dataSource, method, parameters);
	}
}
