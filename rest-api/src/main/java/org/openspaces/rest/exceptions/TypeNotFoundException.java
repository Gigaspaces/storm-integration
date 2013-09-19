package org.openspaces.rest.exceptions;

/**
 * This exception is used in cases when a client tries to perform an write/update operation
 * on a type that is not registered in space
 * @author Dan Kilman
 */
public class TypeNotFoundException
        extends RuntimeException
{
    private static final long serialVersionUID = 1L;
    private final String typeName;

    public TypeNotFoundException(String typeName) {
        this.typeName = typeName;
    }

    public String getTypeName() {
        return typeName;
    }

}
