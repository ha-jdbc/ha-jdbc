/*
 * HA-JDBC: High-Availablity JDBC
 * Copyright 2010 Paul Ferraro
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
package net.sf.hajdbc.state;

/**
 * @author Paul Ferraro
 */
public interface SerializedDurabilityListener
{
	void beforeInvocation(byte[] transactionId, int phase, int exceptionType);
	
	void afterInvocation(byte[] transactionId, int phase);
	
	void beforeInvoker(byte[] transactionId, int phase, String databaseId);
	
	void afterInvoker(byte[] transactionId, int phase, String databaseId, byte[] result);
}
