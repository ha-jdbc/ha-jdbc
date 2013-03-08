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
package net.sf.hajdbc.io.file;

import net.sf.hajdbc.io.InputSinkStrategy;
import net.sf.hajdbc.io.InputSinkProvider;

/**
 * A file-based input sink provider
 * @author Paul Ferraro
 */
public class FileInputSinkProvider implements InputSinkProvider
{
	@Override
	public InputSinkStrategy<? extends Object> createInputSinkStrategy()
	{
		return new FileInputSinkStrategy();
	}

	@Override
	public String getId()
	{
		return "file";
	}
}
