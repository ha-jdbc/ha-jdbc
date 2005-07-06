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
package net.sf.hajdbc.util;

import java.util.Properties;

import org.jibx.runtime.IUnmarshallingContext;
import org.jibx.runtime.JiBXException;
import org.jibx.runtime.impl.UnmarshallingContext;

/**
 * Customer JiBX unmarshaller for unmarshalling a {@link java.util.Properties} object.
 * 
 * @author  Paul Ferraro
 * @since   1.0
 */
public class PropertiesUnmarshaller implements org.jibx.runtime.IUnmarshaller
{
	private static final String ELEMENT = "property";
	private static final String ATTRIBUTE = "name";
	
	private String uri;
	private String name;

	/**
	 * Constructs a new PropertiesUnmarshaller.
	 */
	public PropertiesUnmarshaller()
	{
		this.uri = null;
		this.name = null;
	}
	
	/**
	 * Constructs a new PropertiesUnmarshaller.
	 * @param uri
	 * @param index 
	 * @param name
	 */
	public PropertiesUnmarshaller(String uri, int index, String name)
	{
		this.uri = uri;
		this.name = name;
	}
    
	/**
	 * @see org.jibx.runtime.IUnmarshaller#isPresent(org.jibx.runtime.IUnmarshallingContext)
	 */
	public boolean isPresent(IUnmarshallingContext context) throws JiBXException
	{
		return context.isAt(this.uri, this.name);
	}

	/**
	 * @see org.jibx.runtime.IUnmarshaller#unmarshal(java.lang.Object, org.jibx.runtime.IUnmarshallingContext)
	 */
	public Object unmarshal(Object object, IUnmarshallingContext ctx) throws JiBXException
	{
		UnmarshallingContext context = (UnmarshallingContext) ctx;
		
		Properties properties = (Properties) object;
		
		if (properties == null)
		{
			properties = new Properties();
		}
		
		if (this.name != null)
		{
			context.parsePastStartTag(this.uri, this.name);
		}
		
		while (context.isAt(this.uri, ELEMENT))
		{
			String name = (String) context.attributeText(this.uri, ATTRIBUTE, null);
			
			context.parsePastStartTag(this.uri, ELEMENT);
			
			String value = context.parseContentText();
			
			properties.put(name, value);
			
			context.parsePastEndTag(this.uri, ELEMENT);
		}
		
		if (this.name != null)
		{
			context.parsePastEndTag(this.uri, this.name);
		}
		
		return properties;
	}
}
