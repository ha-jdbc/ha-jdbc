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

import java.io.File;
import java.io.Serializable;

import javax.xml.transform.Result;
import javax.xml.transform.Source;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;

/**
 * Alternative to {@link URLXMLStreamFactory} and the file:// protocol, since default JDK file url stream handler
 * does not support output.
 * 
 * @author Paul Ferraro
 */
public class FileXMLStreamFactory implements XMLStreamFactory, Serializable
{
	private static final long serialVersionUID = -8857228563490452629L;
	
	private final File file;
	
	public FileXMLStreamFactory(File file)
	{
		this.file = file;
	}
	
	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.xml.XMLStreamFactory#createSource()
	 */
	@Override
	public Source createSource()
	{
		return new StreamSource(this.file);
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.xml.XMLStreamFactory#createResult()
	 */
	@Override
	public Result createResult()
	{
		return new StreamResult(this.file);
	}

	/**
	 * {@inheritDoc}
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString()
	{
		return this.file.getPath();
	}
}
