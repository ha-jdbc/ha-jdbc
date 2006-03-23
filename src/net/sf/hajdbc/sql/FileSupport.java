/*
 * HA-JDBC: High-Availability JDBC
 * Copyright (c) 2004-2006 Paul Ferraro
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
import java.sql.Blob;
import java.sql.Clob;
import java.sql.SQLException;

/**
 * Provides temp file support for serializing data for large objects and streams.
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
	 * Create a file from the specified Blob.
	 * @param blob a binary large object
	 * @return a temporary file
	 * @throws java.sql.SQLException if an IO error occurs
	 */
	public File createFile(Blob blob) throws SQLException;
	
	/**
	 * Create a file from the specified Clob.
	 * @param clob a character large object
	 * @return a temporary file
	 * @throws java.sql.SQLException if an IO error occurs
	 */
	public File createFile(Clob clob) throws SQLException;
	
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
	 * Returns an Blob that reads from the specified file.
	 * @param file a temp file
	 * @return a Blob object
	 * @throws java.sql.SQLException if IO error occurs
	 */
	public Blob getBlob(File file) throws SQLException;
	
	/**
	 * Returns an Clob that reads from the specified file.
	 * @param file a temp file
	 * @return a Clob object
	 * @throws java.sql.SQLException if IO error occurs
	 */
	public Clob getClob(File file) throws SQLException;
	
	/**
	 * Deletes any files created by this object.
	 */
	public void close();
}
