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
package net.sf.hajdbc.logging.slf4j;

import org.slf4j.LoggerFactory;

import net.sf.hajdbc.logging.AbstractLogger;
import net.sf.hajdbc.logging.Level;

/**
 * SLF4J-based {@link net.sf.hajdbc.logging.Logger}.
 * @author Paul Ferraro
 */
public class SLF4JLogger extends AbstractLogger
{
	private final org.slf4j.Logger logger;
	
	public SLF4JLogger(Class<?> targetClass)
	{
		this.logger = LoggerFactory.getLogger(targetClass);
	}
	
	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.logging.Logger#log(net.sf.hajdbc.logging.Level, java.lang.Throwable, java.lang.String, java.lang.Object[])
	 */
	@Override
	public void log(Level level, Throwable e, String pattern, Object... args)
	{
		switch (level)
		{
			case ERROR:
			{
				if (this.logger.isErrorEnabled())
				{
					String message = format(pattern, args);
					
					if (e != null)
					{
						this.logger.error(message, e);
					}
					else
					{
						this.logger.error(message);
					}
				}
				
				break;
			}
			case WARN:
			{
				if (this.logger.isWarnEnabled())
				{
					String message = format(pattern, args);
					
					if (e != null)
					{
						this.logger.warn(message, e);
					}
					else
					{
						this.logger.warn(message);
					}
				}
				
				break;
			}
			case INFO:
			{
				if (this.logger.isInfoEnabled())
				{
					String message = format(pattern, args);
					
					if (e != null)
					{
						this.logger.info(message, e);
					}
					else
					{
						this.logger.info(message);
					}
				}
				
				break;
			}
			case DEBUG:
			{
				if (this.logger.isDebugEnabled())
				{
					String message = format(pattern, args);
					
					if (e != null)
					{
						this.logger.debug(message, e);
					}
					else
					{
						this.logger.debug(message);
					}
				}
				
				break;
			}
			case TRACE:
			{
				if (this.logger.isTraceEnabled())
				{
					String message = format(pattern, args);
					
					if (e != null)
					{
						this.logger.trace(message, e);
					}
					else
					{
						this.logger.trace(message);
					}
				}
				
				break;
			}
		}
	}
}
