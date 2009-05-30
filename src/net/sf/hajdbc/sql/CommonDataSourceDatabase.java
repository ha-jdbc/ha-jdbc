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
import java.util.Properties;

import javax.management.DynamicMBean;
import javax.management.NotCompliantMBeanException;
import javax.management.StandardMBean;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;

import net.sf.hajdbc.Messages;

/**
 * A database described by a data source.
 * @author Paul Ferraro
 * @param <D> <code>javax.sql</code> data source interface
 */
public abstract class CommonDataSourceDatabase<D> extends AbstractDatabase<D> implements InactiveDataSourceDatabaseMBean
{
	private String name;
	private Class<D> targetClass;
	
	protected CommonDataSourceDatabase(Class<D> targetClass)
	{
		this.targetClass = targetClass;
	}
	
	/**
	 * @see net.sf.hajdbc.sql.ActiveDataSourceDatabaseMBean#getName()
	 */
	@Override
	public String getName()
	{
		return this.name;
	}

	/**
	 * @see net.sf.hajdbc.sql.InactiveDataSourceDatabaseMBean#setName(java.lang.String)
	 */
	@Override
	public void setName(String name)
	{
		this.checkDirty(this.name, name);
		this.name = name;
	}

	/**
	 * @see net.sf.hajdbc.Database#createConnectionFactory()
	 */
	@Override
	public D createConnectionFactory()
	{
		try
		{
			Class<?> dataSourceClass = Class.forName(this.name);
			
			return this.createDataSource(dataSourceClass.asSubclass(this.targetClass));
		}
		catch (ClassNotFoundException e)
		{
			return this.createDataSource();
		}
	}

	private D createDataSource()
	{
		try
		{
			Context context = new InitialContext(this.getProperties());
	
			return this.targetClass.cast(context.lookup(this.name));
		}
		catch (NamingException e)
		{
			throw new IllegalArgumentException(Messages.getMessage(Messages.JNDI_LOOKUP_FAILED, this.name), e);
		}
	}

	private D createDataSource(Class<? extends D> dataSourceClass)
	{
		Properties properties = this.getProperties();
		
		try
		{
			D dataSource = dataSourceClass.newInstance();
			
			for (PropertyDescriptor descriptor: Introspector.getBeanInfo(dataSourceClass).getPropertyDescriptors())
			{
				String value = properties.getProperty(descriptor.getName());
				
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
	
	/**
	 * @see net.sf.hajdbc.Database#getActiveMBean()
	 */
	@Override
	public DynamicMBean getActiveMBean()
	{
		try
		{
			return new StandardMBean(this, ActiveDataSourceDatabaseMBean.class);
		}
		catch (NotCompliantMBeanException e)
		{
			throw new IllegalStateException(e);
		}
	}

	/**
	 * @see net.sf.hajdbc.Database#getInactiveMBean()
	 */
	@Override
	public DynamicMBean getInactiveMBean()
	{
		try
		{
			return new StandardMBean(this, InactiveDataSourceDatabaseMBean.class);
		}
		catch (NotCompliantMBeanException e)
		{
			throw new IllegalStateException(e);
		}
	}
}