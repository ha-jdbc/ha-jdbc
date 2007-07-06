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

import java.util.Arrays;
import java.util.Collections;
import java.util.Set;
import java.util.TreeSet;

import net.sf.hajdbc.DatabaseCluster;
import net.sf.hajdbc.StateManager;
import net.sf.hajdbc.util.Strings;

/**
 * @author Paul Ferraro
 */
public class QueryInitialStateCommand implements Command<Set<String>>
{
	private static final long serialVersionUID = -8409746321944635265L;

	private static final String DELIMITER = ",";
	
	/**
	 * @see net.sf.hajdbc.distributable.Command#execute(net.sf.hajdbc.DatabaseCluster, net.sf.hajdbc.StateManager)
	 */
	@Override
	public <D> Set<String> execute(DatabaseCluster<D> databaseCluster, StateManager stateManager)
	{
		return databaseCluster.isActive() ? stateManager.getInitialState() : null;
	}

	/**
	 * Optimize transfer of result by marshalling set of strings into a string.
	 * @see net.sf.hajdbc.distributable.Command#marshalResult(java.lang.Object)
	 */
	@Override
	public Object marshalResult(Set<String> set)
	{
		return (set != null) ? Strings.join(set, DELIMITER) : null;
	}

	/**
	 * Restore marshalled string into a set of strings
	 * @see net.sf.hajdbc.distributable.Command#unmarshalResult(java.lang.Object)
	 */
	@Override
	public Set<String> unmarshalResult(Object object)
	{
		String state = (String) object;
		
		if (state == null) return null;
		
		if (state.length() == 0) return Collections.emptySet();
		
		return new TreeSet<String>(Arrays.asList(state.split(DELIMITER)));
	}
	
	/**
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString()
	{
		return this.getClass().getName();
	}
}
