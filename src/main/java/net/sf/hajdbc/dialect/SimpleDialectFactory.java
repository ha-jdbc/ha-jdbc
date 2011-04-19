/*
 * HA-JDBC: High-Availablity JDBC
 * Copyright 2011 Paul Ferraro
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
package net.sf.hajdbc.dialect;

import net.sf.hajdbc.Dialect;

/**
 * @author Paul Ferraro
 */
public class SimpleDialectFactory implements DialectFactory
{
	private final String vendor;
	
	public SimpleDialectFactory(String url)
	{
		int start = url.indexOf(':') + 1;
		this.vendor = url.substring(start, url.indexOf(':', start));
	}
	
	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.dialect.DialectFactory#createDialect()
	 */
	@Override
	public Dialect createDialect()
	{
		return DialectFactoryEnum.valueOf(this.vendor.toUpperCase()).createDialect();
	}
}
