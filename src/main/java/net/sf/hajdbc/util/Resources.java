package net.sf.hajdbc.util;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import net.sf.hajdbc.logging.Level;
import net.sf.hajdbc.logging.Logger;
import net.sf.hajdbc.logging.LoggerFactory;

public class Resources
{
	private static Logger logger = LoggerFactory.getLogger(Resources.class);
	
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

	public static void close(Closeable resource)
	{
		if (resource == null) return;
		try
		{
			resource.close();
		}
		catch (IOException e)
		{
			logger.log(Level.WARN, e);
		}
	}
	
	public static void close(ObjectInput input)
	{
		if (input == null) return;
		try
		{
			input.close();
		}
		catch (IOException e)
		{
			logger.log(Level.WARN, e);
		}
	}
	
	public static void close(ObjectOutput output)
	{
		if (output == null) return;
		try
		{
			output.close();
		}
		catch (IOException e)
		{
			logger.log(Level.WARN, e);
		}
	}
	
	public static void close(Connection connection)
	{
		if (connection == null) return;
		try
		{
			if (!connection.isClosed())
			{
				connection.close();
			}
		}
		catch (SQLException e)
		{
			logger.log(Level.WARN, e);
		}
	}
	
	public static void close(Statement statement)
	{
		if (statement == null) return;
		try
		{
			statement.close();
		}
		catch (SQLException e)
		{
			logger.log(Level.WARN, e);
		}
	}
	
	public static void close(ResultSet resultSet)
	{
		if (resultSet == null) return;
		try
		{
			resultSet.close();
		}
		catch (SQLException e)
		{
			logger.log(Level.WARN, e);
		}
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
