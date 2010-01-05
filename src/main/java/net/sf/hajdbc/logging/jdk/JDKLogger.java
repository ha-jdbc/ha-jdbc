/*
 * HA-JDBC: High-Availability JDBC
 * Copyright 2004-2009 Paul Ferraro
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
package net.sf.hajdbc.logging.jdk;

import java.util.IdentityHashMap;
import java.util.Map;

import net.sf.hajdbc.logging.AbstractLogger;
import net.sf.hajdbc.logging.Level;

/**
 * @author paul
 *
 */
public class JDKLogger extends AbstractLogger
{
	private static final Map<Level, java.util.logging.Level> levels = new IdentityHashMap<Level, java.util.logging.Level>();
	static
	{
		levels.put(Level.ERROR, java.util.logging.Level.SEVERE);
		levels.put(Level.WARN, java.util.logging.Level.WARNING);
		levels.put(Level.INFO, java.util.logging.Level.INFO);
		levels.put(Level.DEBUG, java.util.logging.Level.FINE);
		levels.put(Level.TRACE, java.util.logging.Level.FINEST);
	}
	
	private java.util.logging.Logger logger;
	
	public JDKLogger(Class<?> targetClass)
	{
		this.logger = java.util.logging.Logger.getLogger(targetClass.getName());
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.logging.Logger#log(net.sf.hajdbc.logging.Level, java.lang.String, java.lang.Throwable, java.lang.Object[])
	 */
	@Override
	public void log(Level level, Throwable e, String pattern, Object... args)
	{
		java.util.logging.Level realLevel = levels.get(level);
		
		if (this.logger.isLoggable(realLevel))
		{
			String message = format(pattern, args);
			
			if (e != null)
			{
				this.logger.log(realLevel, message, e);
			}
			else
			{
				this.logger.log(realLevel, message);
			}
		}
	}
}
