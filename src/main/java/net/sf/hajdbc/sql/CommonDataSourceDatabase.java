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

import java.beans.IntrospectionException;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.beans.PropertyEditor;
import java.beans.PropertyEditorManager;
import java.lang.reflect.InvocationTargetException;
import java.util.Map;
import java.util.Properties;

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;

import net.sf.hajdbc.Messages;
import net.sf.hajdbc.management.Managed;

/**
 * A database described by a data source.
 * @author Paul Ferraro
 * @param <Z> <code>javax.sql</code> data source interface
 */
public abstract class CommonDataSourceDatabase<Z extends javax.sql.CommonDataSource> extends AbstractDatabase<Z>
{
	protected abstract Class<Z> getTargetClass();
	
	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.sql.AbstractDatabase#getName()
	 */
	@Managed(description = "The JNDI name to which this DataSource is bound, or the class name of the DataSource implementation")
	@Override
	public void setName(String name)
	{
		super.setName(name);
	}

	/**
	 * @see net.sf.hajdbc.Database#createConnectionSource()
	 */
	@Override
	public Z createConnectionSource()
	{
		try
		{
			Class<?> dataSourceClass = Class.forName(this.getName());
			
			return this.createDataSource(dataSourceClass.asSubclass(this.getTargetClass()));
		}
		catch (ClassNotFoundException e)
		{
			return this.createDataSource();
		}
	}

	private Z createDataSource()
	{
		Properties properties = new Properties();
		
		for (Map.Entry<String, String> entry: this.getProperties().entrySet())
		{
			properties.setProperty(entry.getKey(), entry.getValue());
		}

		String name = this.getName();
		
		try
		{
			Context context = new InitialContext(properties);
	
			return this.getTargetClass().cast(context.lookup(name));
		}
		catch (NamingException e)
		{
			throw new IllegalArgumentException(Messages.JNDI_LOOKUP_FAILED.getMessage(name), e);
		}
	}

	private Z createDataSource(Class<? extends Z> dataSourceClass)
	{
		Map<String, String> properties = this.getProperties();
		
		try
		{
			Z dataSource = dataSourceClass.newInstance();
			
			for (PropertyDescriptor descriptor: Introspector.getBeanInfo(dataSourceClass).getPropertyDescriptors())
			{
				String value = properties.get(descriptor.getName());
				
				if (value != null)
				{
					PropertyEditor editor = PropertyEditorManager.findEditor(descriptor.getPropertyType());
					
					editor.setAsText(value);
					
					descriptor.getWriteMethod().invoke(dataSource, editor.getValue());
				}
			}
			
			return dataSource;
		}
		catch (InstantiationException e)
		{
			throw new IllegalArgumentException(e.toString(), e);
		}
		catch (IllegalAccessException e)
		{
			throw new IllegalArgumentException(e.toString(), e);
		}
		catch (IntrospectionException e)
		{
			throw new IllegalArgumentException(e.toString(), e);
		}
		catch (InvocationTargetException e)
		{
			throw new IllegalArgumentException(e.getTargetException().toString(), e);
		}
	}
}