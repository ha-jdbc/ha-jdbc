package net.sf.hajdbc.util;

import java.io.Closeable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import net.sf.hajdbc.logging.Level;
import net.sf.hajdbc.logging.Logger;
import net.sf.hajdbc.logging.LoggerFactory;

public class Resources
{
	private static final Logger logger = LoggerFactory.getLogger(Resources.class);
	
	public static void close(Closeable resource)
	{
		try
		{
			resource.close();
		}
		catch (IOException e)
		{
			logger.log(Level.WARN, e);
		}
	}
	
	public static void close(ObjectOutput output)
	{
		try
		{
			output.close();
		}
		catch (IOException e)
		{
			logger.log(Level.WARN, e);
		}
	}
	
	public static void close(ObjectInput input)
	{
		try
		{
			input.close();
		}
		catch (IOException e)
		{
			logger.log(Level.WARN, e);
		}
	}
	
	public static void close(Connection connection)
	{
		try
		{
			connection.close();
		}
		catch (SQLException e)
		{
			logger.log(Level.WARN, e);
		}
	}
	
	public static void close(Statement statement)
	{
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
		try
		{
			resultSet.close();
		}
		catch (SQLException e)
		{
			logger.log(Level.WARN, e);
		}
	}
	
	private Resources()
	{
		// Hide
	}
}
