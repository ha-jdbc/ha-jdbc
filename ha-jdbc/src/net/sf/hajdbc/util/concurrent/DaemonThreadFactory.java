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
	public Thread newThread(Runnable runnable)
	{
		Thread thread = new Thread(runnable);
		
		thread.setDaemon(true);

		return thread;
	}
}
