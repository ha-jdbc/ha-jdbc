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
package net.sf.hajdbc.sync;

import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.beans.PropertyEditor;
import java.beans.PropertyEditorManager;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

import net.sf.hajdbc.Messages;
import net.sf.hajdbc.SQLException;
import net.sf.hajdbc.SynchronizationStrategy;

/**
 * @author  Paul Ferraro
 * @since   1.0
 */
public abstract class AbstractSynchronizationStrategy implements SynchronizationStrategy
{
	protected String createForeignKeySQL = ForeignKey.DEFAULT_CREATE_SQL;
	protected String dropForeignKeySQL = ForeignKey.DEFAULT_DROP_SQL;
	protected int fetchSize = 0;
	protected String id;
	
	/**
	 * @see net.sf.hajdbc.SynchronizationStrategy#getId()
	 */
	public String getId()
	{
		return this.id;
	}
	
	/**
	 * Sets the identifier
	 * @param id an identifier
	 */
	public void setId(String id)
	{
		this.id = id;
	}
	
	/**
	 * @return the createForeignKeySQL.
	 */
	public String getCreateForeignKeySQL()
	{
		return this.createForeignKeySQL;
	}
	
	/**
	 * @param createForeignKeySQL the createForeignKeySQL to set.
	 */
	public void setCreateForeignKeySQL(String createForeignKeySQL)
	{
		this.createForeignKeySQL = createForeignKeySQL;
	}
	
	/**
	 * @return the dropForeignKeySQL.
	 */
	public String getDropForeignKeySQL()
	{
		return this.dropForeignKeySQL;
	}
	
	/**
	 * @param dropForeignKeySQL the dropForeignKeySQL to set.
	 */
	public void setDropForeignKeySQL(String dropForeignKeySQL)
	{
		this.dropForeignKeySQL = dropForeignKeySQL;
	}

	/**
	 * @return the fetchSize.
	 */
	public int getFetchSize()
	{
		return this.fetchSize;
	}

	/**
	 * @param fetchSize the fetchSize to set.
	 */
	public void setFetchSize(int fetchSize)
	{
		this.fetchSize = fetchSize;
	}
	
	/**
	 * @see net.sf.hajdbc.SynchronizationStrategy#writeProperties(java.util.Properties)
	 */
	public void writeProperties(Properties properties) throws Exception
	{
		if (!properties.isEmpty())
		{
			PropertyDescriptor[] descriptors = Introspector.getBeanInfo(this.getClass()).getPropertyDescriptors();
			
			Map propertyDescriptorMap = new HashMap(descriptors.length);
			
			for (int i = 0; i < descriptors.length; ++i)
			{
				PropertyDescriptor descriptor = descriptors[i];
				
				if (descriptor.getName().equals("class")) continue;
				
				propertyDescriptorMap.put(descriptor.getName(), descriptor);
			}
			
			Iterator names = properties.keySet().iterator();
			
			while (names.hasNext())
			{
				String name = (String) names.next();
				
				PropertyDescriptor descriptor = (PropertyDescriptor) propertyDescriptorMap.get(name);
				
				if (descriptor == null)
				{
					throw new SQLException(Messages.getMessage(Messages.INVALID_PROPERTY, new Object[] { name, this }));
				}
				
				PropertyEditor editor = PropertyEditorManager.findEditor(descriptor.getPropertyType());
				
				String textValue = properties.getProperty(name);
				
				try
				{
					if (editor == null)
					{
						throw new IllegalArgumentException();
					}

					editor.setAsText(textValue);
				}
				catch (IllegalArgumentException e)
				{
					throw new SQLException(Messages.getMessage(Messages.INVALID_PROPERTY_VALUE, new Object[] { textValue, name, this }));
				}
				
				descriptor.getWriteMethod().invoke(this, new Object[] { editor.getValue() });
			}
		}
	}
	
	/**
	 * @see net.sf.hajdbc.SynchronizationStrategy#readProperties()
	 */
	public Properties readProperties() throws Exception
	{
		Properties properties = new Properties();
		
		PropertyDescriptor[] descriptors = Introspector.getBeanInfo(this.getClass()).getPropertyDescriptors();
		
		for (int i = 0; i < descriptors.length; ++i)
		{
			PropertyDescriptor descriptor = descriptors[i];
			
			if (descriptor.getName().equals("class")) continue;
			
			PropertyEditor editor = PropertyEditorManager.findEditor(descriptor.getPropertyType());
			
			if (editor == null) continue;
			
			editor.setValue(descriptor.getReadMethod().invoke(this, null));
			
			properties.setProperty(descriptor.getName(), editor.getAsText());
		}
		
		return properties;
	}
	
	/**
	 * @see java.lang.Object#toString()
	 */
	public String toString()
	{
		return this.id;
	}
	
	/**
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	public boolean equals(Object object)
	{
		SynchronizationStrategy strategy = (SynchronizationStrategy) object;
		
		return this.id.equals(strategy.getId());
	}
}
