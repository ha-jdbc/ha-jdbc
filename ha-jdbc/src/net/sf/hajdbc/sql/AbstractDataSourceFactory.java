/*
 * HA-JDBC: High-Availability JDBC
 * Copyright (C) 2004 Paul Ferraro
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

import java.util.Hashtable;

import javax.naming.Context;
import javax.naming.Name;
import javax.naming.Reference;
import javax.naming.spi.ObjectFactory;

/**
 * @author  Paul Ferraro
 * @version $Revision$
 * @since   1.0
 */
public abstract class AbstractDataSourceFactory implements ObjectFactory
{
	/**
	 * @see javax.naming.spi.ObjectFactory#getObjectInstance(java.lang.Object, javax.naming.Name, javax.naming.Context, java.util.Hashtable)
	 */
	public Object getObjectInstance(Object object, Name name, Context context, Hashtable environment) throws Exception
	{
		if (object == null)
		{
			return null;
		}
		
		if (!Reference.class.isInstance(object))
		{
			return null;
		}
		
		Reference reference = (Reference) object;
		
		String className = reference.getClassName();
		
		if (className == null)
		{
			return null;
		}
		
		Class objectClass = this.getObjectClass();
		
		if (!objectClass.getName().equals(className))
		{
			return null;
		}
		
		AbstractDataSource dataSource = (AbstractDataSource) objectClass.newInstance();
		
		String clusterName = (String) reference.get(AbstractDataSource.CLUSTER_NAME).getContent();
		
		dataSource.setName(clusterName);
		
		return object;
	}
	
	protected abstract Class getObjectClass();
}
