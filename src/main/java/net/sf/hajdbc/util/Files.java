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
package net.sf.hajdbc.util;

import java.io.File;
import java.io.IOException;
import java.security.AccessController;
import java.security.PrivilegedAction;

public class Files
{
	public static File createTempFile(final String suffix) throws IOException
	{
		PrivilegedAction<File> action = new PrivilegedAction<File>()
		{
			@Override
			public File run()
			{
				try
				{
					return File.createTempFile("ha-jdbc_", suffix);
				}
				catch (IOException e)
				{
					throw new PrivilegedIOException(e);
				}
			}
		};
		try
		{
			return AccessController.doPrivileged(action);
		}
		catch (PrivilegedIOException e)
		{
			throw e.getException();
		}
	}
	
	public static void delete(final File file)
	{
		PrivilegedAction<Void> action = new PrivilegedAction<Void>()
		{
			@Override
			public Void run()
			{
				if (!file.delete())
				{
					file.deleteOnExit();
				}
				return null;
			}
		};
		AccessController.doPrivileged(action);
	}
	private static class PrivilegedIOException extends RuntimeException
	{
		private static final long serialVersionUID = 7017527313040459676L;
		private final IOException e;
		
		PrivilegedIOException(IOException e)
		{
			this.e = e;
		}
		
		IOException getException()
		{
			return this.e;
		}
	}
}
