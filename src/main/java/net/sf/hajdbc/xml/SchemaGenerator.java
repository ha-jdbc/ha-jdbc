package net.sf.hajdbc.xml;

import java.io.File;
import java.io.IOException;
import java.text.MessageFormat;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.SchemaOutputResolver;
import javax.xml.transform.Result;
import javax.xml.transform.stream.StreamResult;

public class SchemaGenerator
{
	public static final String NAMESPACE = "urn:sourceforge:ha-jdbc:2.1";
	
	public static void main(String... args) throws Throwable
	{
		try
		{
			assert (args.length != 3) : MessageFormat.format("Usage: java {0} <base-class-name> <base-directory> <filename>", SchemaGenerator.class.getName());
			
			String baseClassName = args[0];
			String baseDirectoryName = args[1];
			String fileName = args[2];
			
			Class<?> baseClass = Class.forName(baseClassName);
			File baseDirectory = new File(baseDirectoryName);
			
			final File file = new File(baseDirectory, fileName);
			
			SchemaOutputResolver resolver = new SchemaOutputResolver()
			{
				@Override
				public Result createOutput(String namespaceUri, String suggestedFileName) throws IOException
				{
					return new StreamResult(file);
				}
			};
	
			System.out.println("Generating schema to " + file.getPath());
			
			JAXBContext.newInstance(baseClass).generateSchema(resolver);
		}
		catch (Throwable e)
		{
			e.printStackTrace(System.err);
			throw e;
		}
	}
}
