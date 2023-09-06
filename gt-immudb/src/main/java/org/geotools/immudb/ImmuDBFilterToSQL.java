package org.geotools.immudb;

import org.geotools.data.jdbc.FilterToSQL;
import org.geotools.filter.FilterCapabilities;
import org.geotools.filter.function.InFunction;
import org.geotools.geometry.jts.JTS;
import org.geotools.jdbc.PrimaryKeyColumn;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Polygon;
import org.opengis.feature.type.AttributeDescriptor;
import org.opengis.filter.ExcludeFilter;
import org.opengis.filter.Id;
import org.opengis.filter.IncludeFilter;
import org.opengis.filter.PropertyIsBetween;
import org.opengis.filter.PropertyIsLike;
import org.opengis.filter.PropertyIsNull;
import org.opengis.filter.expression.Add;
import org.opengis.filter.expression.Divide;
import org.opengis.filter.expression.Literal;
import org.opengis.filter.expression.Multiply;
import org.opengis.filter.expression.PropertyName;
import org.opengis.filter.expression.Subtract;
import org.opengis.filter.identity.Identifier;

import java.io.IOException;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

public class ImmuDBFilterToSQL extends FilterToSQL {

    protected List<Object> literalValues = new ArrayList<>();

    protected List<Class> literalTypes = new ArrayList<>();
    protected List<Integer> SRIDs = new ArrayList<>();
    protected List<Integer> dimensions = new ArrayList<>();
    protected List<AttributeDescriptor> descriptors = new ArrayList<>();
    boolean prepareEnabled = true;

    public ImmuDBFilterToSQL(Writer out) {
        super(out);
    }

    @Override
    public Object visit(Literal expression, Object context) throws RuntimeException {
        if (!prepareEnabled) return super.visit(expression, context);

        Class clazz = getTargetClassFromContext(context);

        // evaluate the literal and store it for later
        Object literalValue = evaluateLiteral(expression, clazz);

        // bbox filters have a right side expression that's a ReferencedEnvelope,
        // but SQL dialects use/want polygons instead
        if (literalValue instanceof Envelope && convertEnvelopeToPolygon()) {
            clazz = Polygon.class;
            literalValue = JTS.toGeometry((Envelope) literalValue);
        }

        if (clazz == null && literalValue != null) {
            clazz = literalValue.getClass();
        }

        literalValues.add(literalValue);
        SRIDs.add(currentSRID);
        dimensions.add(currentDimension);
        descriptors.add(
                context instanceof AttributeDescriptor ? (AttributeDescriptor) context : null);
        literalTypes.add(clazz);

        try {
            out.write("?");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return context;
    }

    /**
     * When returning true, the {@link Literal} visit will turn {@link Envelope} objects (typically
     * coming from {@link org.opengis.filter.spatial.BBOX} filters) into {@link Polygon}. Defaults
     * to true, subclasses can override.
     */
    protected boolean convertEnvelopeToPolygon() {
        return true;
    }

    private Class getTargetClassFromContext(Object context) {
        if (context instanceof Class) {
            return (Class) context;
        } else if (context instanceof AttributeDescriptor) {
            return ((AttributeDescriptor) context).getType().getBinding();
        }
        return null;
    }

    /**
     * Encodes an Id filter
     *
     * @param filter the
     * @throws RuntimeException If there's a problem writing output
     */
    @Override
    public Object visit(Id filter, Object extraData) {

        if (primaryKey == null) {
            throw new RuntimeException("Must set a primary key before trying to encode FIDFilters");
        }

        Set ids = filter.getIdentifiers();

        List<PrimaryKeyColumn> columns = primaryKey.getColumns();
        for (Iterator i = ids.iterator(); i.hasNext(); ) {
            try {
                Identifier id = (Identifier) i.next();
                List<Object> attValues = ImmuDBDataStore.decodeFID(primaryKey, id.toString(), false);

                out.write("(");

                for (int j = 0; j < attValues.size(); j++) {
                    // in case of join the pk columns need to be qualified with alias
                    out.write(escapeName(columns.get(j).getName()));
                    out.write(" = ");
                    out.write('?');

                    // store the value for later usage
                    literalValues.add(attValues.get(j));
                    // no srid, pk are not formed with geometry values
                    SRIDs.add(-1);
                    dimensions.add(-1);
                    // if it's not null, we can also infer the type
                    literalTypes.add(attValues.get(j) != null ? attValues.get(j).getClass() : null);
                    descriptors.add(null);

                    if (j < (attValues.size() - 1)) {
                        out.write(" AND ");
                    }
                }

                out.write(")");

                if (i.hasNext()) {
                    out.write(" OR ");
                }
            } catch (java.io.IOException e) {
                throw new RuntimeException(IO_ERROR, e);
            }
        }

        return extraData;
    }

    public List<Object> getLiteralValues() {
        return literalValues;
    }

    public List<Class> getLiteralTypes() {
        return literalTypes;
    }

    /**
     * Returns the list of native SRID for each literal that happens to be a geometry, or null
     * otherwise
     */
    public List<Integer> getSRIDs() {
        return SRIDs;
    }

    /**
     * Returns the list of dimensions for each literal tha happens to be a geometry, or null
     * otherwise
     */
    public List<Integer> getDimensions() {
        return dimensions;
    }

    /**
     * Returns the attribute descriptors compared to a given literal (if any, not always available,
     * normally only needed if arrays are involved)
     */
    public List<AttributeDescriptor> getDescriptors() {
        return descriptors;
    }

    @Override
    protected FilterCapabilities createFilterCapabilities() {
        return createCapabilities();
    }

    static FilterCapabilities createCapabilities(){
        FilterCapabilities capabilities = new FilterCapabilities();
        capabilities.addAll(InFunction.getInCapabilities());
        capabilities.addType(Add.class);
        capabilities.addType(Subtract.class);
        capabilities.addType(Divide.class);
        capabilities.addType(Multiply.class);
        capabilities.addType(PropertyName.class);
        capabilities.addType(Literal.class);
        capabilities.addAll(FilterCapabilities.LOGICAL_OPENGIS);
        capabilities.addAll(FilterCapabilities.SIMPLE_COMPARISONS_OPENGIS);
        capabilities.addType(PropertyIsNull.class);
        capabilities.addType(PropertyIsBetween.class);
        capabilities.addType(PropertyIsLike.class);
        capabilities.addType(Id.class);
        capabilities.addType(IncludeFilter.class);
        capabilities.addType(ExcludeFilter.class);
        return capabilities;
    }
}
