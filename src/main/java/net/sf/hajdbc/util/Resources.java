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
