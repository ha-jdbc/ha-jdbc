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
package net.sf.hajdbc.distributed;

import java.io.Serializable;

import net.sf.hajdbc.Identifiable;


/**
 * Factory for creating instances of {@link CommandDispatcher}.
 * 
 * @author Paul Ferraro
 */
public interface CommandDispatcherFactory extends Identifiable, Serializable
{
	/**
	 * Create a new dispatcher for remote command execution.
	 * @param <C> execution context type
	 * @param id unique identifier of this dispatcher
	 * @param context command execution context
	 * @param stateful handler for state transfers
	 * @param membershipListener handler for group membership changes
	 * @return a new command dispatcher
	 * @throws Exception if an error occurred creating the dispatcher
	 */
	<C> CommandDispatcher<C> createCommandDispatcher(String id, C context, Stateful stateful, MembershipListener membershipListener) throws Exception;
}
