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
package net.sf.hajdbc.codec.crypto;

import java.security.GeneralSecurityException;
import java.security.Key;
import java.sql.SQLException;

import javax.crypto.Cipher;

import net.sf.hajdbc.codec.Codec;
import net.sf.hajdbc.sql.SQLExceptionFactory;

import org.apache.commons.codec.binary.Base64;

/**
 * Generic cryptographic codec.
 * @author Paul Ferraro
 */
public class CipherCodec implements Codec
{
	private final Key key;
	
	public CipherCodec(Key key)
	{
		this.key = key;
	}
	
	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.codec.Codec#decrypt(java.lang.String, java.lang.Object)
	 */
	@Override
	public String decode(String value) throws SQLException
	{
		try
		{
			Cipher cipher = Cipher.getInstance(this.key.getAlgorithm());

			cipher.init(Cipher.DECRYPT_MODE, this.key);
			
			return new String(cipher.doFinal(Base64.decodeBase64(value.getBytes())));
		}
		catch (GeneralSecurityException e)
		{
			throw SQLExceptionFactory.getInstance().createException(e);
		}
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.codec.Codec#encode(java.lang.String)
	 */
	@Override
	public String encode(String value) throws SQLException
	{
		try
		{
			Cipher cipher = Cipher.getInstance(this.key.getAlgorithm());

			cipher.init(Cipher.ENCRYPT_MODE, this.key);
			
			return new String(Base64.encodeBase64(cipher.doFinal(value.getBytes())));
		}
		catch (GeneralSecurityException e)
		{
			throw SQLExceptionFactory.getInstance().createException(e);
		}
	}
}
