/*
 * HA-JDBC: High-Availability JDBC
 * Copyright (c) 2004-2008 Paul Ferraro
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

import java.util.EventListener;

/**
 * Event listener for synchronization notifications.
 * @author Paul Ferraro
 */
public interface SynchronizationListener extends EventListener
{
	/**
	 * Triggered just before synchronization commences.
	 * @param event identifies the database to be synchronized.
	 */
	public void beforeSynchronization(DatabaseEvent event);
	
	/**
	 * Triggered immediately after synchronization completes.
	 * @param event identifies the database to be synchronized.
	 */
	public void afterSynchronization(DatabaseEvent event);
}
