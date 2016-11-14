/**
 * Created by jeremyjiang on 2016/6/17.
 * School of EECS, Peking University
 * Copyright (c) All Rights Reserved
 */
package cn.pku.net.db.storm.ndvr.lsh;

import java.util.Random;

/**
 * Description:
 *
 * @author jeremyjiang
 *         Created at 2016/6/17 10:21
 */

public class EuclideanHash implements HashFunction{

    private static final long serialVersionUID = -3784656820380622717L;
    private Vector randomProjection;
    private int offset;
    private int w;

    public EuclideanHash(int dimensions, int w) {
        Random rand = new Random();
        this.w = w;
        this.offset = rand.nextInt(w);

        randomProjection = new Vector(dimensions);
        for (int d = 0; d < dimensions; d++) {
            //mean 0
            //standard deviation 1.0
            double val = rand.nextGaussian();
            randomProjection.set(d, val);
        }
    }

    @Override
    public int hash(Vector vector) {
        double hashValue = (vector.dot(randomProjection)+offset)/Double.valueOf(w);
        return (int) Math.round(hashValue);
    }
}