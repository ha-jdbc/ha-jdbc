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

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;

import net.sf.hajdbc.io.InputSinkChannel;
import net.sf.hajdbc.util.Files;
import net.sf.hajdbc.util.Resources;

/**
 * Input stream channel for writing to, and reading from, a file sink.
 * @author Paul Ferraro
 */
public class FileInputStreamSinkChannel implements InputSinkChannel<InputStream, File>
{
	@Override
	public File write(InputStream input) throws IOException
	{
		File file = Files.createTempFile(FileInputSinkStrategy.TEMP_FILE_SUFFIX);
		FileOutputStream output = new FileOutputStream(file);
		try
		{
			FileChannel fileChannel = output.getChannel();
			ReadableByteChannel inputChannel = Channels.newChannel(input);
			
			ByteBuffer buffer = ByteBuffer.allocate(BUFFER_SIZE);
			
			while (inputChannel.read(buffer) > 0)
			{
				buffer.flip();
				fileChannel.write(buffer);
				buffer.compact();
			}
			
			return file;
		}
		finally
		{
			Resources.close(output);
		}
	}

	@Override
	public InputStream read(File sink) throws IOException
	{
		return new BufferedInputStream(new FileInputStream(sink), BUFFER_SIZE);
	}
}
