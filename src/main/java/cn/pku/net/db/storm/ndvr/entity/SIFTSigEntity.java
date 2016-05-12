/**
 * @Package cn.pku.net.db.storm.ndvr.entity
 * Created by jeremyjiang on 2016/5/12.
 * School of EECS, Peking University
 * Copyright (c) All Rights Reserved
 */



package cn.pku.net.db.storm.ndvr.entity;

import java.util.ArrayList;
import java.util.List;

import cn.pku.net.db.storm.ndvr.image.analyze.sift.scale.KDFeaturePoint;

/**
 * Description: SIFT signature entity
 *
 * @author jeremyjiang
 * Created at 2016/5/12 18:45
 */
public class SIFTSigEntity {
    private List<KDFeaturePoint> sig = new ArrayList<KDFeaturePoint>();

    /**
     * Instantiates
     *
     * @param sig the sig
     */
    public SIFTSigEntity(List<KDFeaturePoint> sig) {
        this.sig = sig;
    }

    /**
     * The entry point of application.
     *
     * @param args the input arguments
     */
    public static void main(String[] args) {}

    /**
     * Getter method for property <tt>sig</tt>.
     *
     * @return property value of sig
     */
    public List<KDFeaturePoint> getSig() {
        return sig;
    }

    /**
     * Setter method for property <tt>sig</tt>.
     *
     * @param sig value to be assigned to property sig
     */
    public void setSig(List<KDFeaturePoint> sig) {
        this.sig = sig;
    }
}


//~ Formatted by Jindent --- http://www.jindent.com
