/*
 * HA-JDBC: High-Availability JDBC
 * Copyright (C) 2014  Paul Ferraro
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
package net.sf.hajdbc.configuration;

import java.beans.IntrospectionException;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.beans.PropertyEditor;
import java.beans.PropertyEditorManager;
import java.lang.reflect.InvocationTargetException;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.HashMap;
import java.util.Map;

import net.sf.hajdbc.Identifiable;
import net.sf.hajdbc.messages.Messages;
import net.sf.hajdbc.messages.MessagesFactory;

public class ServiceBuilder<T extends Identifiable> extends SimpleServiceBuilder<T> implements PropertiesBuilder<T>
{
	private static final Messages messages = MessagesFactory.getMessages();

	private final Map<String, String> properties = new HashMap<>();

	public ServiceBuilder(Class<T> serviceClass, String id)
	{
		super(serviceClass, id);
	}

	@Override
	public ServiceBuilder<T> property(String name, String value)
	{
		this.properties.put(name, value);
		return this;
	}
	
	@Override
	public ServiceBuilder<T> read(T service)
	{
		super.read(service);
		for (Map.Entry<String, Map.Entry<PropertyDescriptor, PropertyEditor>> entry: findDescriptors(service.getClass()).entrySet())
		{
			Map.Entry<PropertyDescriptor, PropertyEditor> descriptorEntry = entry.getValue();
			PropertyDescriptor descriptor = descriptorEntry.getKey();
			PropertyEditor editor = descriptorEntry.getValue();
			try
			{
				Object value = descriptor.getReadMethod().invoke(service);
				editor.setValue(value);
				this.properties.put(entry.getKey(), editor.getAsText());
			}
			catch (IllegalAccessException e)
			{
				throw new IllegalStateException(e);
			}
			catch (InvocationTargetException e)
			{
				throw new IllegalStateException(e.getTargetException());
			}
		}
		return this;
	}

	@Override
	public T build()
	{
		T service = super.build();
		if (!this.properties.isEmpty())
		{
			Map<String, Map.Entry<PropertyDescriptor, PropertyEditor>> descriptors = findDescriptors(service.getClass());
			for (Map.Entry<String, String> property: this.properties.entrySet())
			{
				String name = property.getKey();
				String value = property.getValue();
				Map.Entry<PropertyDescriptor, PropertyEditor> entry = descriptors.get(name);
				if (entry == null)
				{
					throw new IllegalArgumentException(messages.invalidJavaBeanProperty(service.getClass(), name));
				}
				PropertyDescriptor descriptor = entry.getKey();
				PropertyEditor editor = entry.getValue();
	
				try
				{
					editor.setAsText(value);
				}
				catch (IllegalArgumentException e)
				{
					throw new IllegalArgumentException(messages.invalidJavaBeanPropertyValue(descriptor, value), e);
				}

				try
				{
					descriptor.getWriteMethod().invoke(service, editor.getValue());
				}
				catch (IllegalAccessException e)
				{
					throw new IllegalStateException(e);
				}
				catch (InvocationTargetException e)
				{
					throw new IllegalStateException(e.getTargetException());
				}
			}
		}
		return service;
	}
	
	static Map<String, Map.Entry<PropertyDescriptor, PropertyEditor>> findDescriptors(Class<?> targetClass)
	{
		Map<String, Map.Entry<PropertyDescriptor, PropertyEditor>> map = new HashMap<>();
		try
		{
			for (PropertyDescriptor descriptor: Introspector.getBeanInfo(targetClass).getPropertyDescriptors())
			{
				if ((descriptor.getReadMethod() != null) && (descriptor.getWriteMethod() != null))
				{
					PropertyEditor editor = PropertyEditorManager.findEditor(descriptor.getPropertyType());
					
					if (editor != null)
					{
						map.put(descriptor.getName(), new SimpleImmutableEntry<>(descriptor, editor));
					}
				}
			}
			
			return map;
		}
		catch (IntrospectionException e)
		{
			throw new IllegalStateException(e);
		}
	}
}
