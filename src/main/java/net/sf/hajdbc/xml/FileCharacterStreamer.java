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

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Reader;
import java.io.Serializable;
import java.io.Writer;

/**
 * Alternative to {@link URLCharacterStreamer}, since default JDK file url stream handler
 * does not support output.
 * 
 * @author Paul Ferraro
 */
public class FileCharacterStreamer implements CharacterStreamer, Serializable
{
	private static final long serialVersionUID = -8857228563490452629L;
	
	private final File file;
	
	public FileCharacterStreamer(File file)
	{
		this.file = file;
	}
	
	@Override
	public Reader getReader() throws IOException
	{
		return new FileReader(this.file);
	}

	@Override
	public Writer getWriter() throws IOException
	{
		return new FileWriter(this.file);
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
