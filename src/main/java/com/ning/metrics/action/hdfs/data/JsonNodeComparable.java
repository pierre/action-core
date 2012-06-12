/*
 * Copyright 2010-2011 Ning, Inc.
 *
 * Ning licenses this file to you under the Apache License, version 2.0
 * (the "License"); you may not use this file except in compliance with the
 * License.  You may obtain a copy of the License at:
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package com.ning.metrics.action.hdfs.data;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.JsonNode;

import java.util.Iterator;
import java.util.List;

public class JsonNodeComparable extends JsonNode implements Comparable
{
    private JsonNode delegate;

    public JsonNodeComparable(JsonNode delegate)
    {
        this.delegate = delegate;
    }

    @Override
    public int compareTo(Object o)
    {
        JsonNode thing = (JsonNode) o;

        if (isMissingNode() && !thing.isMissingNode()) {
            return -1;
        }
        else if (!isMissingNode() && thing.isMissingNode()) {
            return 1;
        }

        int mySize = 0;
        Iterator<JsonNode> myIterator = elements();
        while (myIterator.hasNext()) {
            mySize++;
        }

        int hisSize = 0;
        Iterator<JsonNode> hisIterator = thing.elements();
        while (hisIterator.hasNext()) {
            hisSize++;
        }


        if (mySize != 0 || hisSize != 0) {
            if (mySize > hisSize) {
                return 1;
            }
            else if (mySize < hisSize) {
                return -1;
            }
        }

        // Looks like both nodes don't have children
        if (isValueNode() && thing.isValueNode()) {
            return textValue().compareTo(thing.textValue());
        }
        else {
            if (equals(thing)) {
                return 0;
            }
            else {
                // Weak. When do we come here?
                return toString().compareTo(thing.toString());
            }
        }
    }

    /**
     * Method that can be used for efficient type detection
     * when using stream abstraction for traversing nodes.
     * Will return the first {@link com.fasterxml.jackson.core.JsonToken} that equivalent
     * stream event would produce (for most nodes there is just
     * one token but for structured/container types multiple)
     *
     * @since 1.3
     */
    @Override
    public JsonToken asToken()
    {
        return delegate.asToken();
    }

    /**
     * If this node is a numeric type (as per {@link #isNumber}),
     * returns native type that node uses to store the numeric
     * value.
     */
    @Override
    public JsonParser.NumberType numberType()
    {
        return delegate.numberType();
    }

    /**
     * Method that will return valid String representation of
     * the container value, if the node is a value node
     * (method {@link #isValueNode} returns true), otherwise
     * empty String.
     *
     * @since 1.9 (replaces <code>getValueAsText</code>)
     */
    @Override
    public String asText()
    {
        return delegate.asText();
    }

    /**
     * Method that will return valid String representation of
     * the container value, if the node is a value node
     * (method {@link #isValueNode} returns true), otherwise null.
     * <p/>
     * Note: to serialize nodes of any type, you should call
     * {@link #toString} instead.
     */
    @Override
    public String textValue()
    {
        return delegate.textValue();
    }

    /**
     * Method for finding a JSON Object field with specified name in this
     * node or its child nodes, and returning value it has.
     * If no matching field is found in this node or its descendants, returns null.
     *
     * @param fieldName Name of field to look for
     * @return Value of first matching node found, if any; null if none
     * @since 1.6
     */
    @Override
    public JsonNode findValue(String fieldName)
    {
        return delegate.findValue(fieldName);
    }

    /**
     * Method similar to {@link #findValue}, but that will return a
     * "missing node" instead of null if no field is found. Missing node
     * is a specific kind of node for which {@link #isMissingNode}
     * returns true; and all value access methods return empty or
     * missing value.
     *
     * @param fieldName Name of field to look for
     * @return Value of first matching node found; or if not found, a
     *         "missing node" (non-null instance that has no value)
     * @since 1.6
     */
    @Override
    public JsonNode findPath(String fieldName)
    {
        return delegate.findPath(fieldName);
    }

    /**
     * Method for finding a JSON Object that contains specified field,
     * within this node or its descendants.
     * If no matching field is found in this node or its descendants, returns null.
     *
     * @param fieldName Name of field to look for
     * @return Value of first matching node found, if any; null if none
     * @since 1.6
     */
    @Override
    public JsonNode findParent(String fieldName)
    {
        return delegate.findParent(fieldName);
    }

    @Override
    public List<JsonNode> findValues(String fieldName, List<JsonNode> foundSoFar)
    {
        return delegate.findValues(fieldName, foundSoFar);
    }

    @Override
    public List<String> findValuesAsText(String fieldName, List<String> foundSoFar)
    {
        return delegate.findValuesAsText(fieldName, foundSoFar);
    }

    @Override
    public List<JsonNode> findParents(String fieldName, List<JsonNode> foundSoFar)
    {
        return delegate.findParents(fieldName, foundSoFar);
    }

    /**
     * This method is similar to {@link #get(String)}, except
     * that instead of returning null if no such value exists (due
     * to this node not being an object, or object not having value
     * for the specified field),
     * a "missing node" (node that returns true for
     * {@link #isMissingNode}) will be returned. This allows for
     * convenient and safe chained access via path calls.
     */
    @Override
    public JsonNode path(String fieldName)
    {
        return delegate.path(fieldName);
    }

    /**
     * This method is similar to {@link #get(int)}, except
     * that instead of returning null if no such element exists (due
     * to index being out of range, or this node not being an array),
     * a "missing node" (node that returns true for
     * {@link #isMissingNode}) will be returned. This allows for
     * convenient and safe chained access via path calls.
     */
    @Override
    public JsonNode path(int index)
    {
        return delegate.path(index);
    }

    /**
     * Method for constructing a {@link com.fasterxml.jackson.core.JsonParser} instance for
     * iterating over contents of the tree that this
     * node is root of.
     */
    @Override
    public JsonParser traverse()
    {
        return delegate.traverse();
    }

    /**
     * <p/>
     * Note: marked as abstract to ensure all implementation
     * classes define it properly.
     */
    @Override
    public String toString()
    {
        return delegate.toString();
    }

    /**
     * Equality for node objects is defined as full (deep) value
     * equality. This means that it is possible to compare complete
     * JSON trees for equality by comparing equality of root nodes.
     * <p/>
     * Note: marked as abstract to ensure all implementation
     * classes define it properly and not rely on definition
     * from {@link Object}.
     */
    @Override
    public boolean equals(Object o)
    {
        return delegate.equals(o);
    }

    @Override
    public <T extends JsonNode> T deepCopy()
    {
        return (T) delegate;
    }

    public JsonNode getDelegate()
    {
        return delegate;
    }
}
