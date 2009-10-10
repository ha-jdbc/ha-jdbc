/*
 * HA-JDBC: High-Availability JDBC
 * Copyright (c) 2004-2009 Paul Ferraro
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
package net.sf.hajdbc.codec.hex;

import java.sql.SQLException;

import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;

import net.sf.hajdbc.codec.AbstractCodec;
import net.sf.hajdbc.sql.SQLExceptionFactory;

/**
 * Codec that uses hex encoding/decoding.
 * @author Paul Ferraro
 */
public class HexCodecFactory extends AbstractCodec
{
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
			throw SQLExceptionFactory.getInstance().createException(e);
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
