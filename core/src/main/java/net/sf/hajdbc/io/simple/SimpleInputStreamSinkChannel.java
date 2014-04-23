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
package net.sf.hajdbc.io.simple;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;

import net.sf.hajdbc.io.InputSinkChannel;

/**
 * Input stream channel for writing to, and reading from, an in-memory buffer sink.
 * @author Paul Ferraro
 */
public class SimpleInputStreamSinkChannel implements InputSinkChannel<InputStream, byte[]>
{
	@Override
	public byte[] write(InputStream input) throws IOException
	{
		ByteArrayOutputStream output = new ByteArrayOutputStream();
		int b = input.read();
		while (b >= 0)
		{
			output.write(b);
			b = input.read();
		}
		return output.toByteArray();
	}

	@Override
	public InputStream read(byte[] sink)
	{
		return new ByteArrayInputStream(sink);
	}
}
