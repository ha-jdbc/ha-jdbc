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
package net.sf.hajdbc.codec.hex;

import java.sql.SQLException;

import net.sf.hajdbc.ExceptionType;
import net.sf.hajdbc.codec.AbstractCodec;

import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;

/**
 * Codec that uses hex encoding/decoding.
 * @author Paul Ferraro
 */
public class HexCodecFactory extends AbstractCodec
{
	private static final long serialVersionUID = 5273729775503057299L;

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.codec.Codec#decode(java.lang.String)
	 */
	@Override
	public String decode(String value) throws SQLException
	{
		try
		{
			return new String(Hex.decodeHex(value.toCharArray()));
		}
		catch (DecoderException e)
		{
			throw ExceptionType.getExceptionFactory(SQLException.class).createException(e);
		}
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.codec.Codec#encode(java.lang.String)
	 */
	@Override
	public String encode(String value)
	{
		return new String(Hex.encodeHex(value.getBytes()));
	}
}
