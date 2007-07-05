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
package net.sf.hajdbc.distributable;

import net.sf.hajdbc.DatabaseCluster;
import net.sf.hajdbc.StateManager;


/**
 * Command pattern object indicating that a database is to be deactivated.
 * @author  Paul Ferraro
 * @since   1.0
 */
public class DeactivateCommand extends AbstractCommand
{
	private static final long serialVersionUID = 3257006574802647092L;

	/**
	 * Constructs a new DatabaseDeactivationCommand.
	 */
	public DeactivateCommand()
	{
		super();
	}

	/**
	 * Constructs a new DatabaseDeactivationCommand.
	 * @param databaseId a database identifier
	 */
	public DeactivateCommand(String databaseId)
	{
		super(databaseId);
	}

	/**
	 * @see net.sf.hajdbc.distributable.Command#execute(net.sf.hajdbc.DatabaseCluster)
	 */
	@Override
	public <D> Boolean execute(DatabaseCluster<D> databaseCluster, StateManager stateManager)
	{
		return databaseCluster.deactivate(databaseCluster.getDatabase(this.databaseId), stateManager);
	}
}
