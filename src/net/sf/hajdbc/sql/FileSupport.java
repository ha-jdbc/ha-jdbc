/*
 * HA-JDBC: High-Availability JDBC
 * Copyright (c) 2004-2007 Paul Ferraro
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

import java.io.File;
import java.io.InputStream;
import java.io.Reader;
import java.sql.SQLException;

/**
 * Provides temp file support for serializing data for streams.
 * Any files created by this object are deleted when {@link #close()} is called.
 * 
 * @author  Paul Ferraro
 * @version $Revision$
 * @since   1.0
 */
public interface FileSupport
{
	/**
	 * Create a file from the specified binary input stream.
	 * @param inputStream a binary stream of data
	 * @return a temporary file
	 * @throws java.sql.SQLException if an IO error occurs
	 */
	public File createFile(InputStream inputStream) throws SQLException;
	
	/**
	 * Create a file from the specified character input stream
	 * @param reader a character stream of data
	 * @return a temporary file
	 * @throws java.sql.SQLException if an IO error occurs
	 */
	public File createFile(Reader reader) throws SQLException;

	/**
	 * Returns a reader for the specified file.
	 * @param file a temp file
	 * @return a reader
	 * @throws java.sql.SQLException if IO error occurs
	 */
	public Reader getReader(File file) throws SQLException;
	
	/**
	 * Returns an input stream for the specified file.
	 * @param file a temp file
	 * @return an input stream
	 * @throws java.sql.SQLException if IO error occurs
	 */
	public InputStream getInputStream(File file) throws SQLException;
	
	/**
	 * Deletes any files created by this object.
	 */
	public void close();
}
