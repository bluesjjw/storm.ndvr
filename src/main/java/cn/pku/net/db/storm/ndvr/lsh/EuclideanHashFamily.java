/**
 * Created by jeremyjiang on 2016/6/17.
 * School of EECS, Peking University
 * Copyright (c) All Rights Reserved
 */
package cn.pku.net.db.storm.ndvr.lsh;

import java.util.Arrays;

/**
 * Description:
 *
 * @author jeremyjiang
 *         Created at 2016/6/17 10:59
 */

public class EuclideanHashFamily implements HashFamily{

    private static final long serialVersionUID = 3406464542795652263L;
    private final int dimensions;
    private int w;

    public EuclideanHashFamily(int w, int dimensions){
        this.dimensions = dimensions;
        this.w=w;
    }

    @Override
    public HashFunction createHashFunction(){
        return new EuclideanHash(dimensions, w);
    }

    @Override
    public Integer combine(int[] hashes){
        return Arrays.hashCode(hashes);
    }

    @Override
    public DistanceMeasure createDistanceMeasure() {
        return new EuclideanDistance();
    }
}