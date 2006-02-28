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
package net.sf.hajdbc.util.concurrent;

import java.text.ParseException;
import java.util.Date;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.quartz.CronTrigger;

/**
 * Scheduled thread-pool executor service implementation that leverages a Quartz cron trigger to calculate future execution times for scheduled tasks.
 * @author  Paul Ferraro
 * @since   1.1
 */
public class CronThreadPoolExecutor extends ScheduledThreadPoolExecutor implements CronExecutorService
{
	protected static Log log = LogFactory.getLog(CronThreadPoolExecutor.class);
	
	/**
	 * Constructs a new CronThreadPoolExecutor.
	 * @param corePoolSize
	 * @param handler
	 */
	public CronThreadPoolExecutor(int corePoolSize, RejectedExecutionHandler handler)
	{
		super(corePoolSize + 1, handler);
	}

	/**
	 * Constructs a new CronThreadPoolExecutor.
	 * @param corePoolSize
	 * @param factory
	 * @param handler
	 */
	public CronThreadPoolExecutor(int corePoolSize, ThreadFactory factory, RejectedExecutionHandler handler)
	{
		super(corePoolSize + 1, factory, handler);
	}

	/**
	 * Constructs a new CronThreadPoolExecutor.
	 * @param corePoolSize
	 * @param factory
	 */
	public CronThreadPoolExecutor(int corePoolSize, ThreadFactory factory)
	{
		super(corePoolSize + 1, factory);
	}

	/**
	 * Constructs a new CronThreadPoolExecutor.
	 * @param corePoolSize
	 */
	public CronThreadPoolExecutor(int corePoolSize)
	{
		super(corePoolSize + 1);
	}
	
	/**
	 * Schedules the specified task for execution using the specified cron expression
	 * @param runnable
	 * @param expression
	 */
	public void schedule(final Runnable runnable, String expression)
	{
		if (runnable == null) throw new NullPointerException();
		
		final CronTrigger trigger = new CronTrigger();
		
		try
		{
			trigger.setCronExpression(expression);
		}
		catch (ParseException e)
		{
			throw new RejectedExecutionException(e);
		}
		
		Runnable task = new Runnable()
		{
			/**
			 * @see java.lang.Runnable#run()
			 */
			public void run()
			{
				Date fireTime = trigger.getFireTimeAfter(new Date());
			
				while (fireTime != null)
				{
					try
					{
						CronThreadPoolExecutor.this.schedule(runnable, Math.max(fireTime.getTime() - System.currentTimeMillis(), 0), TimeUnit.MILLISECONDS).get();
						
						fireTime = trigger.getFireTimeAfter(new Date());
					}
					catch (ExecutionException e)
					{
						log.warn(e.getMessage(), e.getCause());
					}
					catch (RejectedExecutionException e)
					{
						break;
					}
					catch (CancellationException e)
					{
						break;
					}
					catch (InterruptedException e)
					{
						break;
					}
				}
			}
		};
		
		this.execute(task);
	}
}
