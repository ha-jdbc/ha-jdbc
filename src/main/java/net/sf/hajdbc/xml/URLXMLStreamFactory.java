/*
 * HA-JDBC: High-Availablity JDBC
 * Copyright 2010 Paul Ferraro
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

import java.io.Serializable;
import java.net.URL;

import javax.xml.transform.Result;
import javax.xml.transform.Source;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;

/**
 * @author Paul Ferraro
 */
public class URLXMLStreamFactory implements XMLStreamFactory, Serializable
{
	private static final long serialVersionUID = -3911432025271185584L;
	
	private final String url;
	
	public URLXMLStreamFactory(URL url)
	{
		this.url = url.toString();
	}
	
	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.xml.XMLStreamFactory#createSource()
	 */
	@Override
	public Source createSource()
	{
		return new StreamSource(this.url);
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.xml.XMLStreamFactory#createResult()
	 */
	@Override
	public Result createResult()
	{
		return new StreamResult(this.url);
	}
	
	/**
	 * {@inheritDoc}
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString()
	{
		return this.url;
	}
}
