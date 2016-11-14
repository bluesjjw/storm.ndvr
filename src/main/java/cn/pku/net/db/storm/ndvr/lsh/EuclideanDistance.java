/**
 * Created by jeremyjiang on 2016/6/17.
 * School of EECS, Peking University
 * Copyright (c) All Rights Reserved
 */
package cn.pku.net.db.storm.ndvr.lsh;

/**
 * Description: Calculates the <a
 * href="http://en.wikipedia.org/wiki/Euclidean_distance">Euclidean distance</a>
 * between two vectors. Sometimes this is also called the L2 distance.
 *
 * @author jeremyjiang
 *         Created at 2016/6/17 10:49
 */

public class EuclideanDistance implements DistanceMeasure{

    @Override
    public double distance(Vector one, Vector other) {
        double sum = 0.0;
        for (int d = 0; d < one.getDimensions(); d++) {
            double delta = one.get(d) - other.get(d);
            sum += delta * delta;
        }
        return Math.sqrt(sum);
    }
}