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
package net.sf.hajdbc;

import java.util.Set;

/**
 * @author Paul Ferraro
 *
 */
public interface StateManager extends DatabaseActivationListener, DatabaseDeactivationListener, Lifecycle
{
	/**
	 * Returns the initial state of the cluster.
	 * @return a set of database identifiers, or null, if no initial cluster state was found.
	 */
	public Set<String> getInitialState();
	
	/**
	 * Detects whether or not the membership of this state manager is empty.
	 * Used during cluster panic detection.
	 * @return true, if membership is empty, false otherwise
	 */
	public boolean isMembershipEmpty();
}
