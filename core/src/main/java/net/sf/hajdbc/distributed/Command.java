/*
 * HA-JDBC: High-Availability JDBC
 * Copyright (C) 2012  Paul Ferraro
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

/**
 * A command to be executed on a remote member.
 * 
 * @author Paul Ferraro
 * @param <R> the execution return type
 * @param <C> the execution context type
 */
public interface Command<R, C> extends Serializable
{
	/**
	 * Execute this command with the specified context.
	 * @param context the execution context.
	 * @return the result of this command.
	 */
	R execute(C context);
}
