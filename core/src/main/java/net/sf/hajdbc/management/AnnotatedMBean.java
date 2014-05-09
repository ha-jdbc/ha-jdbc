/*
 * HA-JDBC: High-Availability JDBC
 * Copyright (C) 2012  Paul Ferraro
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
package net.sf.hajdbc.management;

import java.beans.BeanInfo;
import java.beans.Introspector;
import java.beans.MethodDescriptor;
import java.beans.PropertyDescriptor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.management.Attribute;
import javax.management.AttributeList;
import javax.management.AttributeNotFoundException;
import javax.management.DynamicMBean;
import javax.management.InvalidAttributeValueException;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanException;
import javax.management.MBeanInfo;
import javax.management.MBeanOperationInfo;
import javax.management.ReflectionException;

import net.sf.hajdbc.messages.Messages;
import net.sf.hajdbc.messages.MessagesFactory;

/**
 * @author Paul Ferraro
 */
public class AnnotatedMBean implements DynamicMBean
{
	private static final Messages messages = MessagesFactory.getMessages();

	private final MBeanInfo info;
	private final Object bean;
	private final Map<String, Method> accessorMap = new HashMap<>();
	private final Map<String, Method> mutatorMap = new HashMap<>();
	
	public AnnotatedMBean(Object bean)
	{
		Class<?> beanClass = bean.getClass();
		
		MBean mbean = beanClass.getAnnotation(MBean.class);
		
		if (mbean == null)
		{
			throw new IllegalArgumentException(messages.annotationMissing(beanClass, MBean.class));
		}
		
		this.bean = bean;
		
		try
		{
			BeanInfo beanInfo = Introspector.getBeanInfo(beanClass);
			PropertyDescriptor[] properties = beanInfo.getPropertyDescriptors();
			List<MBeanAttributeInfo> attributeList = new ArrayList<>(properties.length);
			
			for (PropertyDescriptor descriptor: properties)
			{
				Method accessor = descriptor.getReadMethod();
				ManagedAttribute managedAccessor = (accessor != null) ? accessor.getAnnotation(ManagedAttribute.class) : null;

				Method mutator = descriptor.getWriteMethod();
				ManagedAttribute managedMutator = (mutator != null) ? mutator.getAnnotation(ManagedAttribute.class) : null;
				
				if ((managedAccessor != null) || (managedMutator != null))
				{
					String name = descriptor.getName();
					
					Description description = (accessor != null) ? accessor.getAnnotation(Description.class) : null;
					if ((description == null) && (mutator != null))
					{
						description = mutator.getAnnotation(Description.class);
					}
					
					attributeList.add(new MBeanAttributeInfo(name, (description != null) ? description.value() : null, (managedAccessor != null) ? accessor : null, (managedMutator != null) ? mutator : null));
					
					if (managedAccessor != null)
					{
						this.accessorMap.put(name, accessor);
					}
					
					if (managedMutator != null)
					{
						this.mutatorMap.put(name, mutator);
					}
				}
			}
			
			MethodDescriptor[] methods = beanInfo.getMethodDescriptors();
			List<MBeanOperationInfo> operationList = new ArrayList<>(methods.length);
			
			for (MethodDescriptor descriptor: beanInfo.getMethodDescriptors())
			{
				Method method = descriptor.getMethod();
				ManagedOperation managedMethod = method.getAnnotation(ManagedOperation.class);
				
				if (managedMethod != null)
				{
					Description description = method.getAnnotation(Description.class);
					
					operationList.add(new MBeanOperationInfo((description != null) ? description.value() : null, method));
				}
			}
			
			Description description = beanClass.getAnnotation(Description.class);
			
			this.info = new MBeanInfo(beanClass.getName(), (description != null) ? description.value() : null, attributeList.toArray(new MBeanAttributeInfo[attributeList.size()]), null, operationList.toArray(new MBeanOperationInfo[operationList.size()]), null);
		}
		catch (java.beans.IntrospectionException e)
		{
			throw new IllegalArgumentException(e);
		}
		catch (javax.management.IntrospectionException e)
		{
			throw new IllegalArgumentException(e);
		}
	}
	
	/**
	 * {@inheritDoc}
	 * @see javax.management.DynamicMBean#getAttribute(java.lang.String)
	 */
	@Override
	public Object getAttribute(String name) throws AttributeNotFoundException, MBeanException, ReflectionException
	{
		Method method = this.accessorMap.get(name);
		
		if (method == null)
		{
			throw new AttributeNotFoundException(name);
		}
		
		try
		{
			return method.invoke(this.bean);
		}
		catch (InvocationTargetException e)
		{
			throw new ReflectionException(e);
		}
		catch (IllegalArgumentException e)
		{
			throw new MBeanException(e);
		}
		catch (IllegalAccessException e)
		{
			throw new MBeanException(e);
		}
	}

	/**
	 * {@inheritDoc}
	 * @see javax.management.DynamicMBean#getAttributes(java.lang.String[])
	 */
	@Override
	public AttributeList getAttributes(String[] names)
	{
		AttributeList list = new AttributeList(names.length);
		
		for (String name: names)
		{
			try
			{
				list.add(new Attribute(name, this.getAttribute(name)));
			}
			catch (Exception e)
			{
				// Ignore
			}
		}

		return list;
	}

	/**
	 * {@inheritDoc}
	 * @see javax.management.DynamicMBean#getMBeanInfo()
	 */
	@Override
	public MBeanInfo getMBeanInfo()
	{
		return this.info;
	}

	/**
	 * {@inheritDoc}
	 * @see javax.management.DynamicMBean#invoke(java.lang.String, java.lang.Object[], java.lang.String[])
	 */
	@Override
	public Object invoke(String method, Object[] args, String[] types) throws MBeanException, ReflectionException
	{
		Class<?>[] classes = new Class<?>[types.length];
		
		try
		{
			for (int i = 0; i < types.length; ++i)
			{
				classes[i] = this.bean.getClass().getClassLoader().loadClass(types[i]);
			}
			
			return this.bean.getClass().getMethod(method, classes).invoke(this.bean, args);
		}
		catch (ClassNotFoundException e)
		{
			throw new MBeanException(e);
		}
		catch (IllegalArgumentException e)
		{
			throw new MBeanException(e);
		}
		catch (SecurityException e)
		{
			throw new MBeanException(e);
		}
		catch (IllegalAccessException e)
		{
			throw new MBeanException(e);
		}
		catch (InvocationTargetException e)
		{
			throw new ReflectionException(e);
		}
		catch (NoSuchMethodException e)
		{
			throw new MBeanException(e);
		}
	}

	/**
	 * {@inheritDoc}
	 * @see javax.management.DynamicMBean#setAttribute(javax.management.Attribute)
	 */
	@Override
	public void setAttribute(Attribute attribute) throws AttributeNotFoundException, InvalidAttributeValueException, MBeanException, ReflectionException
	{
		Method method = this.mutatorMap.get(attribute.getName());
		
		if (method == null)
		{
			throw new AttributeNotFoundException(attribute.getName());
		}
		
		try
		{
			this.mutatorMap.get(attribute.getName()).invoke(this.bean, attribute.getValue());
		}
		catch (IllegalArgumentException e)
		{
			throw new InvalidAttributeValueException(e.getMessage());
		}
		catch (IllegalAccessException e)
		{
			throw new MBeanException(e);
		}
		catch (InvocationTargetException e)
		{
			throw new ReflectionException(e);
		}
	}

	/**
	 * {@inheritDoc}
	 * @see javax.management.DynamicMBean#setAttributes(javax.management.AttributeList)
	 */
	@Override
	public AttributeList setAttributes(AttributeList attributes)
	{
		AttributeList list = new AttributeList(attributes.size());
		
		for (Object attribute: attributes)
		{
			try
			{
				this.setAttribute((Attribute) attribute);
				
				list.add(attribute);
			}
			catch (Exception e)
			{
				// Ignore
			}
		}
		
		return list;
	}
}
