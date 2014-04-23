/*
 * HA-JDBC: High-Availability JDBC
 * Copyright (C) 2013  Paul Ferraro
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
package net.sf.hajdbc.sql.io;

import java.io.IOException;

import net.sf.hajdbc.AbstractExceptionFactory;
import net.sf.hajdbc.ExceptionType;
import net.sf.hajdbc.dialect.Dialect;
import net.sf.hajdbc.durability.Durability.Phase;

/**
 * Factory for IOExceptions
 * @author Paul Ferraro
 */
public class IOExceptionFactory extends AbstractExceptionFactory<IOException>
{
	private static final long serialVersionUID = -8896643332546657350L;

	public IOExceptionFactory()
	{
		super(IOException.class);
	}

	@Override
	public IOException createException(String message)
	{
		return new IOException(message);
	}

	@Override
	public boolean indicatesFailure(IOException exception, Dialect dialect)
	{
		return false;
	}

	@Override
	public ExceptionType getType()
	{
		return ExceptionType.IO;
	}

	@Override
	public boolean correctHeuristic(IOException exception, Phase phase)
	{
		return false;
	}
}
