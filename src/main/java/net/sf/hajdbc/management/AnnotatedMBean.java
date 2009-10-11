/*
 * HA-JDBC: High-Availability JDBC
 * Copyright (c) 2004-2009 Paul Ferraro
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

/**
 * @author Paul Ferraro
 */
public class AnnotatedMBean implements DynamicMBean
{
	private final MBeanInfo info;
	private final Object bean;
	private final Map<String, Method> accessorMap = new HashMap<String, Method>();
	private final Map<String, Method> mutatorMap = new HashMap<String, Method>();
	
	public AnnotatedMBean(Object bean)
	{
		Class<?> beanClass = bean.getClass();
		
		Managed managed = beanClass.getAnnotation(Managed.class);
		
		if (managed == null)
		{
			throw new IllegalArgumentException(String.format("%s is not a @Managed object", bean.getClass()));
		}
		
		this.bean = bean;
		
		try
		{
			BeanInfo beanInfo = Introspector.getBeanInfo(beanClass);
			PropertyDescriptor[] properties = beanInfo.getPropertyDescriptors();
			List<MBeanAttributeInfo> attributeList = new ArrayList<MBeanAttributeInfo>(properties.length);
			
			for (PropertyDescriptor descriptor: properties)
			{
				Method accessor = descriptor.getReadMethod();
				Managed managedAccessor = (accessor != null) ? accessor.getAnnotation(Managed.class) : null;
				String description = (managedAccessor != null) ? managedAccessor.description() : null;
				
				Method mutator = descriptor.getWriteMethod();
				Managed managedMutator = (mutator != null) ? accessor.getAnnotation(Managed.class) : null;
				if (description == null)
				{
					description = (managedMutator != null) ? managedMutator.description() : null;
				}
				
				if ((managedAccessor != null) || (managedMutator != null))
				{
					String name = descriptor.getName();
					accessor = (managedAccessor != null) ? accessor : null;
					mutator = (managedMutator != null) ? mutator : null;
					
					attributeList.add(new MBeanAttributeInfo(descriptor.getName(), description, accessor, mutator));
					
					if (accessor != null)
					{
						this.accessorMap.put(name, accessor);
					}
					
					if (mutator != null)
					{
						this.mutatorMap.put(name, mutator);
					}
				}
			}
			
			MethodDescriptor[] methods = beanInfo.getMethodDescriptors();
			List<MBeanOperationInfo> operationList = new ArrayList<MBeanOperationInfo>(methods.length);
			
			for (MethodDescriptor descriptor: beanInfo.getMethodDescriptors())
			{
				Method method = descriptor.getMethod();
				Managed managedMethod = method.getAnnotation(Managed.class);
				
				if (managedMethod != null)
				{
					operationList.add(new MBeanOperationInfo(managedMethod.description(), method));
				}
			}
			
			this.info = new MBeanInfo(beanClass.getName(), managed.description(), attributeList.toArray(new MBeanAttributeInfo[attributeList.size()]), null, operationList.toArray(new MBeanOperationInfo[operationList.size()]), null);
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
				classes[i] = Class.forName(types[i]);
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
