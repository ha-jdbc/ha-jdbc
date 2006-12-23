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
package net.sf.hajdbc.util;

import java.util.Properties;

import org.jibx.runtime.JiBXException;
import org.jibx.runtime.impl.MarshallingContext;
import org.jibx.runtime.impl.UnmarshallingContext;

/**
 * Customer JiBX unmarshaller for unmarshalling a {@link java.util.Properties} object.
 * 
 * @author  Paul Ferraro
 * @since   1.0
 */
public class PropertiesMapper extends AbstractMapper<Properties>
{
	private static final String ELEMENT = "property";
	private static final String ATTRIBUTE = "name";

	/**
	 * 
	 */
	public PropertiesMapper()
	{
		super();
	}

	/**
	 * @param uri
	 * @param index
	 * @param name
	 */
	public PropertiesMapper(String uri, int index, String name)
	{
		super(uri, index, name);
	}

	/**
	 * @see net.sf.hajdbc.util.AbstractMapper#marshal(java.lang.Object, org.jibx.runtime.impl.MarshallingContext)
	 */
	@Override
	protected void marshal(Properties properties, MarshallingContext context) throws JiBXException
	{
		if (properties != null)
		{
			if (this.name != null)
			{
				context.startTag(this.index, this.name);
			}
			
			for (Object key: properties.keySet())
			{
				String name = String.class.cast(key);
				
				context.startTagAttributes(this.index, ELEMENT).attribute(this.index, ATTRIBUTE, name).closeStartContent().content(properties.getProperty(name)).endTag(this.index, ELEMENT);
			}

			if (this.name != null)
			{
				context.endTag(this.index, this.name);
			}
		}
	}

	/**
	 * @see net.sf.hajdbc.util.AbstractMapper#unmarshal(java.lang.Object, org.jibx.runtime.impl.UnmarshallingContext)
	 */
	@Override
	protected Properties unmarshal(Properties properties, UnmarshallingContext context) throws JiBXException
	{
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
			String name = context.attributeText(this.uri, ATTRIBUTE);
			
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
