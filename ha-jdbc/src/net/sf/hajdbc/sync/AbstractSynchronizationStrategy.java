/*
 * HA-JDBC: High-Availability JDBC
 * Copyright (c) 2004-2006 Paul Ferraro
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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * @author  Paul Ferraro
 * @since   1.0
 */
public abstract class AbstractSynchronizationStrategy implements SynchronizationStrategy
{
	private static Log log = LogFactory.getLog(AbstractSynchronizationStrategy.class);
	
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
			try
			{
				PropertyDescriptor[] descriptors = Introspector.getBeanInfo(this.getClass()).getPropertyDescriptors();
				
				Map<String, PropertyDescriptor> propertyDescriptorMap = new HashMap<String, PropertyDescriptor>();
				
				for (PropertyDescriptor descriptor: descriptors)
				{
					if (descriptor.getName().equals("class")) continue;
					
					propertyDescriptorMap.put(descriptor.getName(), descriptor);
				}
				
				Iterator names = properties.keySet().iterator();
				
				while (names.hasNext())
				{
					String name = (String) names.next();
					
					PropertyDescriptor descriptor = propertyDescriptorMap.get(name);
					
					if (descriptor == null)
					{
						throw new SQLException(Messages.getMessage(Messages.INVALID_PROPERTY, name, this.getClass().getName()));
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
						throw new SQLException(Messages.getMessage(Messages.INVALID_PROPERTY_VALUE, textValue, name, this.getClass().getName()));
					}
					
					descriptor.getWriteMethod().invoke(this, editor.getValue());
				}
			}
			catch (Exception e)
			{
				// JiBX will mask this exception, so log it here.
				log.error(e.getMessage(), e);
				
				throw e;
			}
		}
	}
	
	/**
	 * @see net.sf.hajdbc.SynchronizationStrategy#readProperties()
	 */
	public Properties readProperties() throws Exception
	{
		Properties properties = new Properties();
		
		try
		{
			PropertyDescriptor[] descriptors = Introspector.getBeanInfo(this.getClass()).getPropertyDescriptors();
			
			for (PropertyDescriptor descriptor: descriptors)
			{
				if (descriptor.getName().equals("class")) continue;
				
				PropertyEditor editor = PropertyEditorManager.findEditor(descriptor.getPropertyType());
				
				if (editor == null) continue;
				
				editor.setValue(descriptor.getReadMethod().invoke(this));
				
				properties.setProperty(descriptor.getName(), editor.getAsText());
			}
			
			return properties;
		}
		catch (Exception e)
		{
			// JiBX will mask this exception, so log it here.
			log.error(e.getMessage(), e);
			
			throw e;
		}
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

	/**
	 * @see net.sf.hajdbc.SynchronizationStrategy#requiresTableLocking()
	 */
	public boolean requiresTableLocking()
	{
		return true;
	}
}
