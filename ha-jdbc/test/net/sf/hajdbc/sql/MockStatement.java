/*
 * HA-JDBC: High-Availability JDBC
 * Copyright (C) 2004 Paul Ferraro
 * 
 * This library is free software; you can redistribute it and/or modify it 
 * under the terms of the GNU Lesser General Public License as published by the 
 * Free Software Foundation; either version 2.1 of the License, or (at your 
 * option) any later version.
 * 
 * This library is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or 
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public License 
 * for more details.
 * 
 * You should have received a copy of the GNU Lesser General Public License
 * along with this library; if not, write to the Free Software Foundation, 
 * Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA
 * 
 * Contact: ferraro@users.sourceforge.net
 */
package net.sf.hajdbc.sql;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;

/**
 * @author  Paul Ferraro
 * @version $Revision$
 * @since   1.0
 */
public class MockStatement implements Statement
{

	public ResultSet executeQuery(String arg0) throws SQLException
	{
		return null;
	}

	public int executeUpdate(String arg0) throws SQLException
	{
		return 0;
	}

	public void close() throws SQLException
	{
	}

	public int getMaxFieldSize() throws SQLException
	{
		return 0;
	}

	public void setMaxFieldSize(int arg0) throws SQLException
	{
	}

	public int getMaxRows() throws SQLException
	{
		return 0;
	}

	public void setMaxRows(int arg0) throws SQLException
	{
	}

	public void setEscapeProcessing(boolean arg0) throws SQLException
	{
	}

	public int getQueryTimeout() throws SQLException
	{
		return 0;
	}

	public void setQueryTimeout(int arg0) throws SQLException
	{
	}

	public void cancel() throws SQLException
	{
	}

	public SQLWarning getWarnings() throws SQLException
	{
		return null;
	}

	public void clearWarnings() throws SQLException
	{
	}

	public void setCursorName(String arg0) throws SQLException
	{
	}

	public boolean execute(String arg0) throws SQLException
	{
		return false;
	}

	public ResultSet getResultSet() throws SQLException
	{
		return null;
	}

	public int getUpdateCount() throws SQLException
	{
		return 0;
	}

	public boolean getMoreResults() throws SQLException
	{
		return false;
	}

	public void setFetchDirection(int arg0) throws SQLException
	{
	}

	public int getFetchDirection() throws SQLException
	{
		return 0;
	}

	public void setFetchSize(int arg0) throws SQLException
	{
	}

	public int getFetchSize() throws SQLException
	{
		return 0;
	}

	public int getResultSetConcurrency() throws SQLException
	{
		return 0;
	}

	public int getResultSetType() throws SQLException
	{
		return 0;
	}

	public void addBatch(String arg0) throws SQLException
	{
	}

	public void clearBatch() throws SQLException
	{
	}

	public int[] executeBatch() throws SQLException
	{
		return null;
	}

	public Connection getConnection() throws SQLException
	{
		return null;
	}

	public boolean getMoreResults(int arg0) throws SQLException
	{
		return false;
	}

	public ResultSet getGeneratedKeys() throws SQLException
	{
		return null;
	}

	public int executeUpdate(String arg0, int arg1) throws SQLException
	{
		return 0;
	}

	public int executeUpdate(String arg0, int[] arg1) throws SQLException
	{
		return 0;
	}

	public int executeUpdate(String arg0, String[] arg1) throws SQLException
	{
		return 0;
	}

	public boolean execute(String arg0, int arg1) throws SQLException
	{
		return false;
	}

	public boolean execute(String arg0, int[] arg1) throws SQLException
	{
		return false;
	}

	public boolean execute(String arg0, String[] arg1) throws SQLException
	{
		return false;
	}

	public int getResultSetHoldability() throws SQLException
	{
		return 0;
	}

}
