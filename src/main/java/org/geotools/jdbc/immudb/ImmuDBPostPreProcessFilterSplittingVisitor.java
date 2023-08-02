package org.geotools.jdbc.immudb;

import org.geotools.filter.FilterCapabilities;
import org.geotools.filter.visitor.ClientTransactionAccessor;
import org.geotools.filter.visitor.PostPreProcessFilterSplittingVisitor;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.AttributeDescriptor;
import org.opengis.feature.type.AttributeType;
import org.opengis.filter.expression.PropertyName;

import java.util.Arrays;
import java.util.Date;
import java.util.List;

public class ImmuDBPostPreProcessFilterSplittingVisitor extends PostPreProcessFilterSplittingVisitor {

    private SimpleFeatureType simpleFeatureType;

    private static final List<Class<?>> PRE_TYPES= Arrays.asList(String.class,Integer.class,byte[].class,Boolean.class, Date.class);

    /**
     * Create a new instance.
     *
     * @param fcs                 The FilterCapabilties that describes what Filters/Expressions the server can
     *                            process.
     * @param parent              The FeatureType that this filter involves. Why is this needed?
     * @param transactionAccessor If the transaction is handled on the client and not the server
     *                            then different filters must be sent to the server. This class provides a generic way of
     *                            obtaining the information from the transaction.
     */
    public ImmuDBPostPreProcessFilterSplittingVisitor(FilterCapabilities fcs, SimpleFeatureType parent, ClientTransactionAccessor transactionAccessor) {
        super(fcs, parent, transactionAccessor);
        this.simpleFeatureType=parent;
    }

    @Override
    public Object visit(PropertyName expression, Object notUsed) {
        Object res=expression.evaluate(simpleFeatureType);
        if (res !=null){
            AttributeType attributeType=null;
            if (res instanceof AttributeType)
                attributeType=(AttributeType) res;
            else if  (res instanceof AttributeDescriptor)
                attributeType=((AttributeDescriptor)res).getType();
            if (attributeType==null || isPostType(attributeType.getBinding()))
                postStack.push(expression);
            return null;
        } else {
            return super.visit(expression, notUsed);
        }
    }

    private boolean isPostType(Class<?> binding){
        return binding==null || !PRE_TYPES.stream().anyMatch(c->c.isAssignableFrom(binding));
    }
}
