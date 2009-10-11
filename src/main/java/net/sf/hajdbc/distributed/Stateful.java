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

/**
 * Used by the command dispatcher to handle state transfer between members.
 * 
 * @author Paul Ferraro
 */
public interface Stateful
{
	/**
	 * Returns the state of this object.
	 * @return the current state.
	 */
	byte[] getState();
	
	/**
	 * Sets the state of this object.
	 * @param the state
	 */
	void setState(byte[] state);
}
