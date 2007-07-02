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
package net.sf.hajdbc;

import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.beans.PropertyEditor;
import java.beans.PropertyEditorManager;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * @author  Paul Ferraro
 * @since   1.1
 */
public class SynchronizationStrategyBuilder
{
	private String id;
	private Class<? extends SynchronizationStrategy> targetClass;
	private Properties properties;
	
	/**
	 * Constructs a new SynchronizationStrategyBuilder.
	 */
	public SynchronizationStrategyBuilder()
	{
		// Do nothing
	}
	
	public String getId()
	{
		return this.id;
	}
	
	/**
	 * @return a SynchronizationStrategy instance
	 * @throws Exception
	 */
	public SynchronizationStrategy buildStrategy() throws Exception
	{
		SynchronizationStrategy strategy = this.targetClass.asSubclass(SynchronizationStrategy.class).newInstance();
		
		PropertyDescriptor[] descriptors = Introspector.getBeanInfo(this.targetClass).getPropertyDescriptors();
		
		Map<String, PropertyDescriptor> propertyDescriptorMap = new HashMap<String, PropertyDescriptor>();
		
		for (PropertyDescriptor descriptor: descriptors)
		{
			if (descriptor.getName().equals("class")) continue;
			
			propertyDescriptorMap.put(descriptor.getName(), descriptor);
		}
		
		for (Object key: this.properties.keySet())
		{
			String name = (String) key;
			
			PropertyDescriptor descriptor = propertyDescriptorMap.get(name);
			
			if (descriptor == null)
			{
				throw new IllegalArgumentException(Messages.getMessage(Messages.INVALID_PROPERTY, name, this.getClass().getName()));
			}
			
			PropertyEditor editor = PropertyEditorManager.findEditor(descriptor.getPropertyType());
			
			String textValue = this.properties.getProperty(name);
			
			try
			{
				if (editor == null)
				{
					throw new Exception();
				}

				editor.setAsText(textValue);
			}
			catch (Exception e)
			{
				throw new IllegalArgumentException(Messages.getMessage(Messages.INVALID_PROPERTY_VALUE, textValue, name, this.targetClass.getName()));
			}
			
			descriptor.getWriteMethod().invoke(strategy, editor.getValue());
		}
		
		return strategy;
	}
	
	/**
	 * @param id
	 * @param strategy
	 * @return a builder for this strategy
	 * @throws Exception 
	 */
	public static SynchronizationStrategyBuilder getBuilder(String id, SynchronizationStrategy strategy) throws Exception
	{
		SynchronizationStrategyBuilder builder = new SynchronizationStrategyBuilder();
		
		builder.id = id;
		
		Class<? extends SynchronizationStrategy> strategyClass = strategy.getClass();
		
		builder.targetClass = strategyClass;
		
		builder.properties = new Properties();
		
		PropertyDescriptor[] descriptors = Introspector.getBeanInfo(strategyClass).getPropertyDescriptors();
		
		for (PropertyDescriptor descriptor: descriptors)
		{
			if (descriptor.getName().equals("class")) continue;
			
			PropertyEditor editor = PropertyEditorManager.findEditor(descriptor.getPropertyType());
			
			if (editor == null) continue;
			
			editor.setValue(descriptor.getReadMethod().invoke(strategy));
			
			builder.properties.setProperty(descriptor.getName(), editor.getAsText());
		}
		
		return builder;
	}
}
