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
package net.sf.hajdbc.logging.commons;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import net.sf.hajdbc.logging.AbstractLogger;
import net.sf.hajdbc.logging.Level;

/**
 * Apache Commons logging {@link Logger}.
 * @author Paul Ferraro
 */
public class CommonsLogger extends AbstractLogger
{
	private final Log log;
	
	public CommonsLogger(Class<?> targetClass)
	{
		this.log = LogFactory.getLog(targetClass);
	}
	
	@Override
	public void log(Level level, Throwable e, String pattern, Object... args)
	{
		switch (level)
		{
			case ERROR:
			{
				if (this.log.isErrorEnabled())
				{
					String message = format(pattern, args);
					
					if (e != null)
					{
						this.log.error(message, e);
					}
					else
					{
						this.log.error(message);
					}
				}
				
				break;
			}
			case WARN:
			{
				if (this.log.isWarnEnabled())
				{
					String message = format(pattern, args);
					
					if (e != null)
					{
						this.log.warn(message, e);
					}
					else
					{
						this.log.warn(message);
					}
				}
				
				break;
			}
			case INFO:
			{
				if (this.log.isInfoEnabled())
				{
					String message = format(pattern, args);
					
					if (e != null)
					{
						this.log.info(message, e);
					}
					else
					{
						this.log.info(message);
					}
				}
				
				break;
			}
			case DEBUG:
			{
				if (this.log.isDebugEnabled())
				{
					String message = format(pattern, args);
					
					if (e != null)
					{
						this.log.debug(message, e);
					}
					else
					{
						this.log.debug(message);
					}
				}
				
				break;
			}
			case TRACE:
			{
				if (this.log.isTraceEnabled())
				{
					String message = format(pattern, args);
					
					if (e != null)
					{
						this.log.trace(message, e);
					}
					else
					{
						this.log.trace(message);
					}
				}
				
				break;
			}
		}
	}
}
