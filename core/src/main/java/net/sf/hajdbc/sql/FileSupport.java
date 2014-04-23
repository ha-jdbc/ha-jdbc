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
package net.sf.hajdbc.sql;

import java.io.Closeable;
import java.io.File;
import java.io.InputStream;
import java.io.Reader;
import java.sql.SQLException;

/**
 * Provides temp file support for serializing data for streams.
 * Any files created by this object are deleted when {@link #close()} is called.
 * 
 * @author  Paul Ferraro
 * @version $Revision: 1612 $
 * @since   1.0
 */
public interface FileSupport<E extends Throwable> extends Closeable
{
	/**
	 * Create a file from the specified binary input stream.
	 * @param inputStream a binary stream of data
	 * @return a temporary file
	 * @throws SQLException if an IO error occurs
	 */
	File createFile(InputStream inputStream) throws E;
	
	/**
	 * Create a file from the specified character input stream
	 * @param reader a character stream of data
	 * @return a temporary file
	 * @throws SQLException if an IO error occurs
	 */
	File createFile(Reader reader) throws E;

	/**
	 * Returns a reader for the specified file.
	 * @param file a temp file
	 * @return a reader
	 * @throws SQLException if IO error occurs
	 */
	Reader getReader(File file) throws E;
	
	/**
	 * Returns an input stream for the specified file.
	 * @param file a temp file
	 * @return an input stream
	 * @throws SQLException if IO error occurs
	 */
	InputStream getInputStream(File file) throws E;
}
