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
package net.sf.hajdbc.xml;

import java.io.File;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.SchemaOutputResolver;
import javax.xml.transform.Result;
import javax.xml.transform.stream.StreamResult;

public class SchemaGenerator
{
	public static final String NAMESPACE = "urn:ha-jdbc:cluster:2.1";
	
	public static void main(String... args) throws Throwable
	{
		try
		{
			assert (args.length == 3) : String.format("Usage: java %s <base-class-name> <base-directory> <filename>", SchemaGenerator.class.getName());
			
			String baseClassName = args[0];
			String baseDirectoryName = args[1];
			String fileName = args[2];
			
			Class<?> baseClass = SchemaGenerator.class.getClassLoader().loadClass(baseClassName);
			File baseDirectory = new File(baseDirectoryName);
			
			final File file = new File(baseDirectory, fileName);
			
			SchemaOutputResolver resolver = new SchemaOutputResolver()
			{
				@Override
				public Result createOutput(String namespaceUri, String suggestedFileName)
				{
					return new StreamResult(file);
				}
			};
			
			System.out.println(String.format("Generating schema to %s", file.getPath()));
			
			JAXBContext.newInstance(baseClass).generateSchema(resolver);
		}
		catch (Throwable e)
		{
			e.printStackTrace(System.err);
			throw e;
		}
	}
}
