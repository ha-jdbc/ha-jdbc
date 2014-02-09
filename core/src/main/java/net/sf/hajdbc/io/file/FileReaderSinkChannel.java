/*
 * HA-JDBC: High-Availability JDBC
 * Copyright (C) 2013  Paul Ferraro
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
package net.sf.hajdbc.io.file;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Reader;
import java.io.Writer;
import java.nio.CharBuffer;

import net.sf.hajdbc.io.InputSinkChannel;
import net.sf.hajdbc.util.Files;

/**
 * Reader channel for writing to, and reading from, a file sink..
 * @author Paul Ferraro
 */
public class FileReaderSinkChannel implements InputSinkChannel<Reader, File>
{
	@Override
	public File write(Reader reader) throws IOException
	{
		File file = Files.createTempFile(FileInputSinkStrategy.TEMP_FILE_SUFFIX);
		try (Writer writer = new FileWriter(file))
		{
			CharBuffer buffer = CharBuffer.allocate(BUFFER_SIZE);
			
			while (reader.read(buffer) > 0)
			{
				buffer.flip();
				writer.append(buffer);
				buffer.clear();
			}
			
			return file;
		}
	}

	@Override
	public Reader read(File sink) throws IOException
	{
		return new BufferedReader(new FileReader(sink), BUFFER_SIZE);
	}
}
