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
package net.sf.hajdbc.util.concurrent.cron;

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
	 * @see net.sf.hajdbc.util.concurrent.cron.quartz.CronExpression
	 * @param task the Runnable task to schedule
	 * @param expression a cron expression
	 */
	public void schedule(Runnable task, CronExpression expression);
}
