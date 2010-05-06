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
package net.sf.hajdbc.sql.xa;

import java.lang.reflect.Method;
import java.sql.SQLException;
import java.util.Set;

import javax.sql.XADataSource;

import net.sf.hajdbc.DatabaseCluster;
import net.sf.hajdbc.sql.CommonDataSourceInvocationHandler;
import net.sf.hajdbc.sql.InvocationHandlerFactory;
import net.sf.hajdbc.util.reflect.Methods;

/**
 * @author Paul Ferraro
 *
 */
@SuppressWarnings("nls")
public class XADataSourceInvocationHandler extends CommonDataSourceInvocationHandler<XADataSource, XADataSourceDatabase>
{
	private static final Set<Method> getXAConnectionMethodSet = Methods.findMethods(XADataSource.class, "getXAConnection");
	
	/**
	 * @param databaseCluster
	 */
	public XADataSourceInvocationHandler(DatabaseCluster<XADataSource, XADataSourceDatabase> databaseCluster)
	{
		super(databaseCluster, XADataSource.class);
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.sql.AbstractInvocationHandler#getInvocationHandlerFactory(java.lang.Object, java.lang.reflect.Method, java.lang.Object[])
	 */
	@Override
	protected InvocationHandlerFactory<XADataSource, XADataSourceDatabase, XADataSource, ?, SQLException> getInvocationHandlerFactory(XADataSource object, Method method, Object[] parameters) throws SQLException
	{
		if (getXAConnectionMethodSet.contains(method))
		{
			return new XAConnectionInvocationHandlerFactory();
		}
		
		return super.getInvocationHandlerFactory(object, method, parameters);
	}
}
