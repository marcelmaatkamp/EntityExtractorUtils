/* 
 * Copyright 2002-2005 The Apache Software Foundation
 * Licensed  under the  Apache License,  Version 2.0  (the "License");
 * you may not use  this file  except in  compliance with the License.
 * You may obtain a copy of the License at 
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed  under the  License is distributed on an "AS IS" BASIS,
 * WITHOUT  WARRANTIES OR CONDITIONS  OF ANY KIND, either  express  or
 * implied.
 * 
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.commons.cli.avalon;
//Renamed from org.apache.avalon.excalibur.cli

/**
 * Token handles tokenizing the CLI arguments
 *
 * @version $Revision: 1.2 $ $Date: 2005/03/18 15:26:55 $
 * @since 4.0
 */
class Token
{
    /** Type for a separator token */
    public static final int TOKEN_SEPARATOR = 0;
    /** Type for a text token */
    public static final int TOKEN_STRING = 1;

    private final int m_type;
    private final String m_value;

    /**
     * New Token object with a type and value
     */
    Token( final int type, final String value )
    {
        m_type = type;
        m_value = value;
    }

    /**
     * Get the value of the token
     */
    final String getValue()
    {
        return m_value;
    }

    /**
     * Get the type of the token
     */
    final int getType()
    {
        return m_type;
    }

    /**
     * Convert to a string
     */
    public final String toString()
    {
        final StringBuffer sb = new StringBuffer();
        sb.append( m_type );
        sb.append( ":" );
        sb.append( m_value );
        return sb.toString();
    }
}
