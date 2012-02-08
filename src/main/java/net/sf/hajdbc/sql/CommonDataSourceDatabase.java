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
import net.sf.hajdbc.management.Description;
import net.sf.hajdbc.management.ManagedAttribute;

/**
 * A database described by a data source.
 * @author Paul Ferraro
 * @param <Z> <code>javax.sql</code> data source interface
 */
public abstract class CommonDataSourceDatabase<Z extends javax.sql.CommonDataSource> extends AbstractDatabase<Z>
{
	private final Class<Z> targetClass;
	
	protected CommonDataSourceDatabase(Class<Z> targetClass)
	{
		this.targetClass = targetClass;
	}
	
	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.sql.AbstractDatabase#getName()
	 */
	@ManagedAttribute
	@Description("The JNDI name to which this DataSource is bound, or the class name of the DataSource implementation")
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
			Class<?> dataSourceClass = this.getClass().getClassLoader().loadClass(this.getName());
			
			return this.createDataSource(dataSourceClass.asSubclass(this.targetClass));
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
	
			return this.targetClass.cast(context.lookup(name));
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
