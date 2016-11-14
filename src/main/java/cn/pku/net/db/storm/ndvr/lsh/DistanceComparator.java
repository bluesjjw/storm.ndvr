/**
 * Created by jeremyjiang on 2016/6/17.
 * School of EECS, Peking University
 * Copyright (c) All Rights Reserved
 */
package cn.pku.net.db.storm.ndvr.lsh;

import java.util.Comparator;

/**
 * Description:
 *
 * @author jeremyjiang
 *         Created at 2016/6/17 11:22
 */

public class DistanceComparator implements Comparator<Vector>{

    private final Vector query;
    private final DistanceMeasure distanceMeasure;

    /**
     *
     * @param query The query vector.
     * @param distanceMeasure The distance vector to use.
     */
    public DistanceComparator(Vector query, DistanceMeasure distanceMeasure) {
        this.query = query;
        this.distanceMeasure = distanceMeasure;
    }

    /* (non-Javadoc)
     * @see java.util.Comparator#compare(java.lang.Object, java.lang.Object)
     */
    @Override
    public int compare(Vector one, Vector other) {
        Double oneDistance = distanceMeasure.distance(query,one);
        Double otherDistance = distanceMeasure.distance(query,other);
        return oneDistance.compareTo(otherDistance);
    }
}