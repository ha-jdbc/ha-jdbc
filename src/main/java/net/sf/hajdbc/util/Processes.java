package net.sf.hajdbc.util;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.security.AccessController;
import java.security.PrivilegedExceptionAction;

import net.sf.hajdbc.logging.Level;
import net.sf.hajdbc.logging.Logger;
import net.sf.hajdbc.logging.LoggerFactory;

public class Processes
{
	private static final Logger logger = LoggerFactory.getLogger(Processes.class);

	public static void run(final ProcessBuilder processBuilder) throws Exception
	{
		logger.log(Level.DEBUG, Strings.join(processBuilder.command(), " "));
		
		PrivilegedExceptionAction<Process> action = new PrivilegedExceptionAction<Process>()
		{
			@Override
			public Process run() throws Exception
			{
				return processBuilder.start();
			}
		};
		
		Process process = AccessController.doPrivileged(action);
		
		try
		{
			int status = process.waitFor();
			
			if (status != 0)
			{
				StringBuilder builder = new StringBuilder();
				BufferedReader reader = new BufferedReader(new InputStreamReader(process.getErrorStream()));
				String line = reader.readLine();
				
				while (line != null)
				{
					builder.append(line).append(Strings.NEW_LINE);
					line = reader.readLine();
				}
				
				throw new Exception(builder.toString());
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
