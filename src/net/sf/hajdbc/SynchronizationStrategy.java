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


/**
 * @author  Paul Ferraro
 * @version $Revision$
 * @since   1.0
 */
public interface SynchronizationStrategy
{
	/**
	 * Prepares the specified synchronization context for actual synchronization.
	 * @param context a synchronization context
	 * @throws java.sql.SQLException if prepare fails
	 */
	public void prepare(SynchronizationContext context) throws java.sql.SQLException;
	
	/**
	 * Cleans up the specified synchronization context after synchronization.
	 * @param context a synchronization context
	 * @throws java.sql.SQLException if cleanup fails
	 */
	public void cleanup(SynchronizationContext context);
	
	/**
	 * Synchronizes a target database with a source database as defined by the synchronization context.
	 * @param context a synchronization context
	 * @throws java.sql.SQLException if synchronization fails
	 */
	public void synchronize(SynchronizationContext context) throws java.sql.SQLException;
}
