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

import javax.sql.DataSource;

import net.sf.hajdbc.DatabaseCluster;

/**
 * @author Paul Ferraro
 *
 */
public class DataSourceInvocationHandler extends AbstractInvocationHandler<DataSource, Void, DataSource>
{
	/**
	 * @param databaseCluster
	 */
	public DataSourceInvocationHandler(DatabaseCluster<DataSource> databaseCluster)
	{
		super(databaseCluster, DataSource.class, databaseCluster.getConnectionFactoryMap());
	}

	/**
	 * @see net.sf.hajdbc.sql.AbstractInvocationHandler#getInvocationStrategy(java.lang.Object, java.lang.reflect.Method, java.lang.Object[])
	 */
	@Override
	protected InvocationStrategy<DataSource, DataSource, ?> getInvocationStrategy(DataSource dataSource, Method method, Object[] parameters) throws Exception
	{
		String methodName = method.getName();
		
		if (methodName.equals("getConnection"))
		{
			return new ConnectionInvocationStrategy<DataSource>(dataSource);
		}
		
		if (methodName.startsWith("get"))
		{
			return new DriverReadInvocationStrategy<DataSource, DataSource, Object>();
		}
		
		if (methodName.startsWith("set"))
		{
			return new DriverWriteInvocationStrategy<DataSource, DataSource, Object>();
		}
		
		return super.getInvocationStrategy(dataSource, method, parameters);
	}

	/**
	 * @see net.sf.hajdbc.sql.AbstractInvocationHandler#close(java.lang.Object, java.lang.Object)
	 */
	@Override
	protected void close(Void parent, DataSource dataSource)
	{
		// Do nothing
	}
}
