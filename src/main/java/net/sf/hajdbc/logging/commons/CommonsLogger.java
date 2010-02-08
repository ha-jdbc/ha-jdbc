package net.sf.hajdbc.logging.commons;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import net.sf.hajdbc.logging.AbstractLogger;
import net.sf.hajdbc.logging.Level;

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
