package com.tinkerpop.gremlin.process.graph;

import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Property;

import java.util.Map;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface EdgeTraversal extends ElementTraversal<Edge> {

    public default <E2> GraphTraversal<Edge, Property<E2>> properties(final String... propertyKeys) {
        return (GraphTraversal) this.start().properties(propertyKeys);
    }

    public default <E2> GraphTraversal<Edge, Map<String, Property<E2>>> propertyMap(final String... propertyKeys) {
        return this.start().propertyMap(propertyKeys);
    }

    public default <E2> GraphTraversal<Edge, Map<String, E2>> valueMap(final String... propertyKeys) {
        return this.start().valueMap(propertyKeys);
    }

    public default <E2> GraphTraversal<Edge, Map<String, E2>> valueMap(final boolean includeTokens, final String... propertyKeys) {
        return this.start().valueMap(includeTokens, propertyKeys);
    }

    // necessary so VertexProperty.value() as a non-traversal method works
    public default <E2> GraphTraversal<Edge, E2> value() {
        return this.start().value();
    }
}
