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
package net.sf.hajdbc;

import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.beans.PropertyEditor;
import java.beans.PropertyEditorManager;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

/**
 * Describes a SynchronizationStrategy implementation.
 * 
 * @author  Paul Ferraro
 * @version $Revision$
 * @since   1.0
 */
public class SynchronizationStrategyDescriptor
{
	private String id;
	private String className;
	private Properties properties;
	
	/**
	 * @return the id.
	 */
	public String getId()
	{
		return this.id;
	}
	
	/**
	 * Factory method for creating a SynchronizationStrategy from this descriptor.
	 * @return a SynchronizationStrategy implementation
	 * @throws Exception
	 */
	public SynchronizationStrategy createSynchronizationStrategy() throws Exception
	{
		Class strategyClass = Class.forName(this.className);
		
		if (strategyClass.isAssignableFrom(SynchronizationStrategy.class))
		{
			throw new SQLException(Messages.getMessage(Messages.INVALID_SYNC_STRATEGY, new Object[] { this.className, SynchronizationStrategy.class }));
		}
		
		SynchronizationStrategy strategy = (SynchronizationStrategy) strategyClass.newInstance();

		if ((this.properties != null) && !this.properties.isEmpty())
		{
			PropertyDescriptor[] descriptors = Introspector.getBeanInfo(strategyClass).getPropertyDescriptors();
			
			Map propertyDescriptorMap = new HashMap(descriptors.length);
			
			for (int i = 0; i < descriptors.length; ++i)
			{
				PropertyDescriptor descriptor = descriptors[i];
				
				propertyDescriptorMap.put(descriptor.getName(), descriptor);
			}
			
			Iterator names = this.properties.keySet().iterator();
			
			while (names.hasNext())
			{
				String name = (String) names.next();
				
				PropertyDescriptor descriptor = (PropertyDescriptor) propertyDescriptorMap.get(name);
				
				if (descriptor == null)
				{
					throw new SQLException(Messages.getMessage(Messages.INVALID_PROPERTY, new Object[] { name, this.className }));
				}
				
				PropertyEditor editor = PropertyEditorManager.findEditor(descriptor.getPropertyType());
				
				String textValue = this.properties.getProperty(name);
				
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
					throw new SQLException(Messages.getMessage(Messages.INVALID_PROPERTY_VALUE, new Object[] { textValue, name, this.className }));
				}
				
				descriptor.getWriteMethod().invoke(strategy, new Object[] { editor.getValue() });
			}
		}
		
		return strategy;
	}
}
