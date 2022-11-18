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
import java.nio.file.Files;
import java.security.PrivilegedAction;
import java.security.PrivilegedExceptionAction;

public class Files
{
	public static String TEMP_FILE_PREFIX = "ha-jdbc_";
	
	public static File createTempFile(final String suffix) throws IOException
	{
		PrivilegedExceptionAction<File> action = new PrivilegedExceptionAction<File>()
		{
			@Override
			public File run() throws IOException
			{
				return Files.createTempFile(TEMP_FILE_PREFIX, suffix).toFile();
			}
		};
		
		return Security.run(action, IOException.class);
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

		Security.run(action);
	}
}
