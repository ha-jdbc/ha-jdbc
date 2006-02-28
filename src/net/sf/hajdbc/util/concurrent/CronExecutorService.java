/*
 * Copyright (c) 2004-2006, Identity Theft 911, LLC.  All rights reserved.
 */
package net.sf.hajdbc.util.concurrent;

import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;

/**
 * @author Paul Ferraro
 *
 */
public interface CronExecutorService extends Executor, ExecutorService
{
	public void schedule(Runnable runnable, String expression);
}
