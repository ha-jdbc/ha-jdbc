/*
 * HA-JDBC: High-Availablity JDBC
 * Copyright 2011 Paul Ferraro
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

import javax.xml.transform.Source;
import javax.xml.transform.sax.SAXSource;

import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.XMLReaderFactory;

/**
 * @author Paul Ferraro
 */
public abstract class PropertyFilteredXMLStreamFactory implements XMLStreamFactory
{
	private static final long serialVersionUID = 95078550285162800L;

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.xml.XMLStreamFactory#createSource()
	 */
	@Override
	public Source createSource()
	{
		try
		{
	      XMLReader reader = new PropertyReplacementFilter(XMLReaderFactory.createXMLReader());
	      
	      return new SAXSource(reader, this.createInputSource());
		}
		catch (SAXException e)
		{
			throw new IllegalStateException(e);
		}
	}
	
	protected abstract InputSource createInputSource();
}
