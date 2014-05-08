package net.sf.hajdbc.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.security.PrivilegedExceptionAction;
import java.util.Map;

import net.sf.hajdbc.logging.Level;
import net.sf.hajdbc.logging.Logger;
import net.sf.hajdbc.logging.LoggerFactory;

public class Processes
{
	private static final Logger logger = LoggerFactory.getLogger(Processes.class);

	public static Map<String, String> environment(final ProcessBuilder builder)
	{
		PrivilegedAction<Map<String, String>> action = new PrivilegedAction<Map<String, String>>()
		{
			@Override
			public Map<String, String> run()
			{
				return builder.environment();
			}
		};
		return AccessController.doPrivileged(action);
	}
	
	public static void run(final ProcessBuilder processBuilder) throws Exception
	{
		run(processBuilder, null);
	}

	public static void run(final ProcessBuilder processBuilder, final File input) throws Exception
	{
		processBuilder.redirectErrorStream(true);
		
		logger.log(Level.DEBUG, Strings.join(processBuilder.command(), " "));
		
		PrivilegedExceptionAction<Process> action = new PrivilegedExceptionAction<Process>()
		{
			@Override
			public Process run() throws Exception
			{
				Process process = processBuilder.start();
				if (input != null)
				{
					PrintWriter writer = new PrintWriter(process.getOutputStream());
					BufferedReader reader = new BufferedReader(new FileReader(input));
					try
					{
						String line = reader.readLine();
						while (line != null)
						{
							writer.println(line);
							line = reader.readLine();
						}
					}
					finally
					{
						reader.close();
					}
				}
				return process;
			}
		};

		Process process = AccessController.doPrivileged(action);

		try
		{
			int status = process.waitFor();
			
			BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
			String line = reader.readLine();
			while (line != null)
			{
				logger.log(Level.DEBUG, line);
				line = reader.readLine();
			}
			
			if (status != 0)
			{
				throw new Exception(String.format("%s returned %d", processBuilder.command().get(0), status));
			}
		}
		catch (InterruptedException e)
		{
			Thread.currentThread().interrupt();
			
			throw new Exception(processBuilder.toString(), e);
		}
	}

	private Processes()
	{
		// Hide
	}
}
