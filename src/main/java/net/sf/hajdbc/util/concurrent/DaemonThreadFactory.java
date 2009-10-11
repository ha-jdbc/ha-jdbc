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
package net.sf.hajdbc.util.concurrent;

import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

/**
 * ThreadFactory implementation that creates daemon threads.
 * 
 * @author  Paul Ferraro
 * @since   1.1
 */
public class DaemonThreadFactory implements ThreadFactory
{
	private static ThreadFactory instance = new DaemonThreadFactory();
	
	private ThreadFactory factory = Executors.defaultThreadFactory();
	
	/**
	 * Returns single shared instance
	 * @return a ThreadFactory instance
	 */
	public static ThreadFactory getInstance()
	{
		return instance;
	}
	
	/**
	 * @see java.util.concurrent.ThreadFactory#newThread(java.lang.Runnable)
	 */
	@Override
	public Thread newThread(Runnable runnable)
	{
		Thread thread = this.factory.newThread(runnable);
		
		thread.setDaemon(true);

		return thread;
	}
}
