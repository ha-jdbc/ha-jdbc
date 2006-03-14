/*
 * Copyright (c) 2004-2006, Identity Theft 911, LLC.  All rights reserved.
 */
package net.sf.hajdbc.util.concurrent;

import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;

/**
 * Executor service that schedules a runnable task for execution via a cron expression.
 * 
 * @author Paul Ferraro
 */
public interface CronExecutorService extends Executor, ExecutorService
{
	/**
	 * Schedules the specified task to execute according to the specified cron expression.
	 * @see org.quartz.CronTrigger
	 * @param task the Runnable task to schedule
	 * @param expression a cron expression
	 */
	public void schedule(Runnable task, String expression);
}
