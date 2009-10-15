/*
 * HA-JDBC: High-Availability JDBC
 * Copyright 2004-2009 Paul Ferraro
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

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.Writer;
import java.net.URL;

public class URLCharacterStreamer implements CharacterStreamer
{
	private final URL url;
	
	public URLCharacterStreamer(URL url)
	{
		this.url = url;
	}
	
	@Override
	public Reader getReader() throws IOException
	{
		return new InputStreamReader(this.url.openConnection().getInputStream());
	}

	@Override
	public Writer getWriter() throws IOException
	{
		return new OutputStreamWriter(this.url.openConnection().getOutputStream());
	}
	
	/**
	 * {@inheritDoc}
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString()
	{
		return this.url.toString();
	}
}
