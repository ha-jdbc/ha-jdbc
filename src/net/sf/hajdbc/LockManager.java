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
package net.sf.hajdbc;

import java.util.concurrent.locks.Lock;

/**
 * Manages a set of named read/write locks.  A global lock is represented by an empty name (i.e "").
 * Obtaining a named read or write lock should implicitly obtain a global read lock.
 * Consequently, all named locks are blocked if a global write lock is obtained.
 * @author Paul Ferraro
 */
public interface LockManager
{
	public static final String GLOBAL = "";
	
	/**
	 * Obtains a named read lock.
	 * @param object an object to lock
	 * @return a read lock
	 */
	public Lock readLock(String object);

	/**
	 * Obtains a named write lock.
	 * @param object an object to lock
	 * @return a write lock
	 */
	public Lock writeLock(String object);
	
	public void start() throws Exception;
	
	public void stop();
}