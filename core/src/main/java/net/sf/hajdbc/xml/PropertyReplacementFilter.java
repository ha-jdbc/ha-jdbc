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
package net.sf.hajdbc.xml;

import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import net.sf.hajdbc.util.Strings;
import net.sf.hajdbc.util.SystemProperties;

import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.AttributesImpl;
import org.xml.sax.helpers.XMLFilterImpl;

/**
 * @author Paul Ferraro
 */
public class PropertyReplacementFilter extends XMLFilterImpl
{
	private static final Pattern PATTERN = Pattern.compile("\\$\\{([^\\}]+)\\}");
	private final Properties properties;
	
	public PropertyReplacementFilter()
	{
		super();
		this.properties = SystemProperties.getSystemProperties();
	}
	
	public PropertyReplacementFilter(XMLReader parent)
	{
		this(parent, SystemProperties.getSystemProperties());
	}
	
	public PropertyReplacementFilter(XMLReader parent, Properties properties)
	{
		super(parent);
		this.properties = properties;
	}
	
	/**
	 * {@inheritDoc}
	 * @see org.xml.sax.helpers.XMLFilterImpl#characters(char[], int, int)
	 */
	@Override
	public void characters(char[] data, int start, int length) throws SAXException
	{
		char[] value = this.replace(String.copyValueOf(data, start, length)).toCharArray();
		super.characters(value, 0, value.length);
	}

	/**
	 * {@inheritDoc}
	 * @see org.xml.sax.helpers.XMLFilterImpl#startElement(java.lang.String, java.lang.String, java.lang.String, org.xml.sax.Attributes)
	 */
	@Override
	public void startElement(String uri, String localName, String qName, Attributes attrs) throws SAXException
	{
		AttributesImpl attributes = (attrs instanceof AttributesImpl) ? (AttributesImpl) attrs : new AttributesImpl(attrs);
		
		int length = attributes.getLength();
		for (int i = 0; i < length; ++i)
		{
			attributes.setValue(i, this.replace(attributes.getValue(i)));
		}
		
		super.startElement(uri, localName, qName, attributes);
	}
	
	private String replace(String input)
	{
		StringBuilder builder = new StringBuilder();
		Matcher matcher = PATTERN.matcher(input);

		int tail = 0;
		
		while (matcher.find())
		{
			builder.append(input, tail, matcher.start());

			String group = matcher.group(1);

			if (group.equals("/"))
			{
				builder.append(Strings.FILE_SEPARATOR);
			}
			else if (group.equals(":"))
			{
				builder.append(Strings.PATH_SEPARATOR);
			}
			else
			{
				String key = group;
				String defaultValue = null;
				
				int index = group.indexOf(":");
				
				if (index > 0)
				{
					key = group.substring(0, index);
					defaultValue = group.substring(index + 1);
				}
				
				String value = this.getProperty(key.split(","), defaultValue);

	  			builder.append((value != null) ? value : matcher.group());
			}
			
			tail = matcher.end();
		}
		
		builder.append(input, tail, input.length());
		
		return builder.toString();
	}
	
	private String getProperty(String[] keys, String defaultValue)
	{
		for (String key: keys)
		{
			String value = this.properties.getProperty(key.trim());
			
			if (value != null) return value;
		}

		return defaultValue;
	}
}
