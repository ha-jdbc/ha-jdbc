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
package net.sf.hajdbc.local;

import java.util.Arrays;
import java.util.Collections;
import java.util.Set;
import java.util.TreeSet;
import java.util.prefs.BackingStoreException;
import java.util.prefs.Preferences;

import net.sf.hajdbc.Database;
import net.sf.hajdbc.DatabaseCluster;
import net.sf.hajdbc.StateManager;
import net.sf.hajdbc.Messages;

/**
 * @author Paul Ferraro
 */
public class LocalStateManager implements StateManager
{
	private static final String STATE_DELIMITER = ",";
	
	private static Preferences preferences = Preferences.userNodeForPackage(LocalStateManager.class);
	
	private DatabaseCluster<?> databaseCluster;
	
	public LocalStateManager(DatabaseCluster<?> databaseCluster)
	{
		this.databaseCluster = databaseCluster;
	}

	/**
	 * @see net.sf.hajdbc.StateManager#getInitialState()
	 */
	public Set<String> getInitialState()
	{
		String state = preferences.get(this.databaseCluster.getId(), null);
		
		if (state == null) return null;
		
		if (state.length() == 0) return Collections.emptySet();
		
		return new TreeSet<String>(Arrays.asList(state.split(STATE_DELIMITER)));
	}
	
	/**
	 * @see net.sf.hajdbc.StateManager#add(java.lang.String)
	 */
	public void add(String databaseId)
	{
		this.storeState();
	}

	/**
	 * @see net.sf.hajdbc.StateManager#remove(java.lang.String)
	 */
	public void remove(String databaseId)
	{
		this.storeState();
	}
	
	private void storeState()
	{
		StringBuilder builder = new StringBuilder();
		
		for (Database<?> database: this.databaseCluster.getBalancer().all())
		{
			builder.append(database.getId()).append(STATE_DELIMITER);
		}
		
		builder.deleteCharAt(builder.length() - 1);
		
		preferences.put(this.databaseCluster.getId(), builder.toString());
		
		try
		{
			preferences.flush();
		}
		catch (BackingStoreException e)
		{
			throw new RuntimeException(Messages.getMessage(Messages.CLUSTER_STATE_STORE_FAILED, this.databaseCluster), e);
		}
	}

	/**
	 * @see net.sf.hajdbc.StateManager#start()
	 */
	public void start() throws Exception
	{
		preferences.sync();
	}

	/**
	 * @see net.sf.hajdbc.StateManager#stop()
	 */
	public void stop()
	{
	}
}
