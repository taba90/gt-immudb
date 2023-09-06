package org.geotools.immudb;

import org.geotools.feature.DecoratingFeature;
import org.locationtech.jts.geom.Geometry;
import org.opengis.feature.GeometryAttribute;
import org.opengis.feature.IllegalAttributeException;
import org.opengis.feature.Property;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.AttributeDescriptor;
import org.opengis.feature.type.Name;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class SimpleFeatureDiff extends DecoratingFeature {

    private Set<String> changedAttrs;
    public SimpleFeatureDiff(SimpleFeature delegate) {
        super(delegate);
        this.changedAttrs=new HashSet<>();
    }

    @Override
    public void setAttribute(int position, Object val) {
        super.setAttribute(position, val);
    }

    @Override
    public void setAttribute(Name arg0, Object arg1) {
        super.setAttribute(arg0, arg1);
        changedAttrs.add(arg0.getLocalPart());
    }

    @Override
    public void setAttribute(String path, Object attribute) {
        super.setAttribute(path, attribute);
        changedAttrs.add(path);
    }

    @Override
    public void setAttributes(List<Object> arg0) {
        super.setAttributes(arg0);
        SimpleFeatureType sft=getFeatureType();
        for (int i=0; i<arg0.size(); i++){
            AttributeDescriptor ad=sft.getDescriptor(i);
            if (ad!=null) changedAttrs.add(ad.getLocalName());
        }
    }

    @Override
    public void setAttributes(Object[] arg0) {
        super.setAttributes(arg0);
        SimpleFeatureType sft=getFeatureType();
        for (int i=0; i<arg0.length; i++){
            AttributeDescriptor ad=sft.getDescriptor(i);
            if (ad!=null) changedAttrs.add(ad.getLocalName());
        }
    }

    @Override
    public void setDefaultGeometry(Object arg0) {
        super.setDefaultGeometry(arg0);
        SimpleFeatureType sft=getFeatureType();
        String name=sft.getGeometryDescriptor().getName().getLocalPart();
        changedAttrs.add(name);
    }

    @Override
    public void setDefaultGeometryProperty(GeometryAttribute arg0) {
        super.setDefaultGeometryProperty(arg0);
        changedAttrs.add(arg0.getName().getLocalPart());
    }

    @Override
    public void setDefaultGeometry(Geometry geometry) throws IllegalAttributeException {
        super.setDefaultGeometry(geometry);
        SimpleFeatureType sft=getFeatureType();
        String name=sft.getGeometryDescriptor().getName().getLocalPart();
        changedAttrs.add(name);
    }

    @Override
    public void setValue(Collection<Property> arg0) {
        super.setValue(arg0);
        arg0.forEach(p->changedAttrs.add(p.getName().getLocalPart()));
    }

    @Override
    public void setValue(Object arg0) {
        setValue((Collection<Property>) arg0);
    }

    public Set<String> getDiff(){
        return changedAttrs;
    }

}
