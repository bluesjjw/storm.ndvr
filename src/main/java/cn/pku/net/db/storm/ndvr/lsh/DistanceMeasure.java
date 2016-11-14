/**
 * Created by jeremyjiang on 2016/6/17.
 * School of EECS, Peking University
 * Copyright (c) All Rights Reserved
 */
package cn.pku.net.db.storm.ndvr.lsh;

/**
 * Description: A distance measure defines how distance is calculated, measured as it were, between two vectors.
 * Each hash family has a corresponding distance measure which is abstracted using this interface.
 * @author jeremyjiang
 * Created at 2016/6/17 10:48
 */
public interface DistanceMeasure {

    /**
     * Calculate the distance between two vectors. From one to two.
     * @param one The first vector.
     * @param other The other vector
     * @return A value representing the distance between two vectors.
     */
    double distance(Vector one, Vector other);
}
