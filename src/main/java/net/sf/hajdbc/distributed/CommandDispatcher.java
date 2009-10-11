/*
 * HA-JDBC: High-Availablity JDBC
 * Copyright 2004, 2009 Paul Ferraro
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

import java.util.Map;

import net.sf.hajdbc.Lifecycle;

/**
 * RPC dispatcher that uses the command pattern.
 * @author Paul Ferraro
 *
 * @param <C> the command context type
 */
public interface CommandDispatcher<C> extends Lifecycle
{
	/**
	 * Execute the specified command on the group coordinator.
	 * @param <R> the return value type
	 * @param command the command to execute
	 * @return a map of command execution results per member.
	 */
	<R> Map<Member, R> executeAll(Command<R, C> command);

	/**
	 * Execute the specified command on the coordinator.
	 * @param <R> the return value type
	 * @param command the command to execute
	 * @return the result of the command execution
	 */
	<R> R executeCoordinator(Command<R, C> command);

	/**
	 * Indicates whether or not the local member is the group coordinator.
	 * @return true, if the local member is the group coordinator, false otherwise.
	 */
	boolean isCoordinator();

	/**
	 * Returns the local member.
	 * @return the local member.
	 */
	Member getLocal();
}