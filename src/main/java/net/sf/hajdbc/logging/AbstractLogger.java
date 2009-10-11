/*
 * HA-JDBC: High-Availability JDBC
 * Copyright (c) 2004-2009 Paul Ferraro
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
package net.sf.hajdbc.logging;

import java.text.MessageFormat;

/**
 * @author paul
 *
 */
public abstract class AbstractLogger implements Logger
{
	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.logging.Logger#log(net.sf.hajdbc.logging.Level, java.lang.String, java.lang.Object[])
	 */
	@Override
	public void log(Level level, String pattern, Object... args)
	{
		this.log(level, null, pattern, args);
	}
	
	protected static String format(String pattern, Object... args)
	{
		return (args.length == 0) ? pattern : MessageFormat.format(pattern, args);
	}
}
