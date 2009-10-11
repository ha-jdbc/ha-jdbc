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
package net.sf.hajdbc.sql.xa;

import java.lang.reflect.Method;
import java.sql.SQLException;
import java.util.Set;

import javax.sql.XADataSource;

import net.sf.hajdbc.DatabaseCluster;
import net.sf.hajdbc.sql.CommonDataSourceInvocationHandler;
import net.sf.hajdbc.sql.InvocationStrategy;
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

	@Override
	protected InvocationStrategy<XADataSource, XADataSourceDatabase, XADataSource, ?, SQLException> getInvocationStrategy(XADataSource dataSource, Method method, Object[] parameters) throws SQLException
	{
		if (getXAConnectionMethodSet.contains(method))
		{
			return new XAConnectionInvocationStrategy(this.cluster, dataSource);
		}
		
		return super.getInvocationStrategy(dataSource, method, parameters);
	}
}
