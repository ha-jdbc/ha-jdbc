/*
 * HA-JDBC: High-Availability JDBC
 * Copyright (c) 2004-2007 Paul Ferraro
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

import java.util.Date;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.quartz.CronExpression;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Scheduled thread-pool executor implementation that leverages a Quartz CronExpression to calculate future execution times for scheduled tasks.
 *
 * @author  Paul Ferraro
 * @since   1.1
 */
public class CronThreadPoolExecutor extends ScheduledThreadPoolExecutor implements CronExecutorService
{
	protected static Logger logger = LoggerFactory.getLogger(CronThreadPoolExecutor.class);
	
	/**
	 * Constructs a new CronThreadPoolExecutor.
	 * @param corePoolSize
	 * @param handler
	 */
	public CronThreadPoolExecutor(int corePoolSize, RejectedExecutionHandler handler)
	{
		super(corePoolSize, DaemonThreadFactory.getInstance(), handler);
	}

	/**
	 * Constructs a new CronThreadPoolExecutor.
	 * @param corePoolSize
	 */
	public CronThreadPoolExecutor(int corePoolSize)
	{
		super(corePoolSize, DaemonThreadFactory.getInstance());
	}
	
	/**
	 * @see net.sf.hajdbc.util.concurrent.CronExecutorService#schedule(java.lang.Runnable, org.quartz.CronExpression)
	 */
	@Override
	public void schedule(final Runnable task, final CronExpression expression)
	{
		if (task == null) throw new NullPointerException();
		
		this.setCorePoolSize(this.getCorePoolSize() + 1);
		
		Runnable scheduleTask = new Runnable()
		{
			/**
			 * @see java.lang.Runnable#run()
			 */
			public void run()
			{
				Date time = expression.getNextValidTimeAfter(new Date());
			
				try
				{
					while (time != null)
					{
						long delay = Math.max(time.getTime() - System.currentTimeMillis(), 0);
						
						try
						{
							CronThreadPoolExecutor.this.schedule(task, delay, TimeUnit.MILLISECONDS).get();
							
							time = expression.getNextValidTimeAfter(new Date());
						}
						catch (ExecutionException e)
						{
							logger.warn(e.toString(), e.getCause());
						}
					}
				}
				catch (RejectedExecutionException e)
				{
					// Occurs if executor was already shutdown when schedule() is called
				}
				catch (CancellationException e)
				{
					// Occurs when scheduled, but not yet executed tasks are canceled during shutdown
				}
				catch (InterruptedException e)
				{
					// Occurs when executing tasks are interrupted during shutdownNow()
				}
			}
		};
		
		this.execute(scheduleTask);
	}
}
