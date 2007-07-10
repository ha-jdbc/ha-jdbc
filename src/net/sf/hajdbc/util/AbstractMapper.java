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
package net.sf.hajdbc.util;

import org.jibx.runtime.IAliasable;
import org.jibx.runtime.IMarshaller;
import org.jibx.runtime.IMarshallingContext;
import org.jibx.runtime.IUnmarshaller;
import org.jibx.runtime.IUnmarshallingContext;
import org.jibx.runtime.JiBXException;
import org.jibx.runtime.impl.MarshallingContext;
import org.jibx.runtime.impl.UnmarshallingContext;

/**
 * @author Paul Ferraro
 *
 */
public abstract class AbstractMapper<T> implements IMarshaller, IUnmarshaller, IAliasable
{
	protected String uri;
	protected String name;
	protected int index;
	private Class<T> targetClass;
	
	/**
	 * Constructs a new PropertiesMapper.
	 */
	protected AbstractMapper(Class<T> targetClass)
	{
		this.targetClass = targetClass;
	}
	
	/**
	 * Constructs a new PropertiesMapper.
	 * @param uri
	 * @param index 
	 * @param name
	 */
	protected AbstractMapper(Class<T> targetClass, String uri, int index, String name)
	{
		this(targetClass);
		
		this.uri = uri;
		this.index = index;
		this.name = name;
	}
	
	/**
	 * @see org.jibx.runtime.IMarshaller#isExtension(int)
	 */
	@Override
	public boolean isExtension(int arg0)
	{
		return false;
	}

	/**
	 * @see org.jibx.runtime.IMarshaller#marshal(java.lang.Object, org.jibx.runtime.IMarshallingContext)
	 */
	@Override
	public void marshal(Object object, IMarshallingContext context) throws JiBXException
	{
		this.marshal(this.targetClass.cast(object), (MarshallingContext) context);
	}

	protected abstract void marshal(T object, MarshallingContext context) throws JiBXException;
	
	/**
	 * @see org.jibx.runtime.IUnmarshaller#isPresent(org.jibx.runtime.IUnmarshallingContext)
	 */
	@Override
	public boolean isPresent(IUnmarshallingContext context) throws JiBXException
	{
		return context.isAt(this.uri, this.name);
	}

	/**
	 * @see org.jibx.runtime.IUnmarshaller#unmarshal(java.lang.Object, org.jibx.runtime.IUnmarshallingContext)
	 */
	@Override
	public Object unmarshal(Object object, IUnmarshallingContext context) throws JiBXException
	{
		return this.unmarshal(this.targetClass.cast(object), (UnmarshallingContext) context);
	}

	protected abstract T unmarshal(T object, UnmarshallingContext context) throws JiBXException;
}
