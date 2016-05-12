/**
 * Created by jeremyjiang on 2016/5/12.
 * School of EECS, Peking University
 * Copyright (c) All Rights Reserved
 */
package cn.pku.net.db.storm.ndvr.util;

import cn.pku.net.db.storm.ndvr.entity.HSVSigEntity;

/**
 * Description:
 *
 * @author jeremyjiang
 *         Created at 2016/5/12 21:02
 */

public class SigSim {

    /**
     * Gets global visual signature's distance (HSV)
     *
     * @param queryHSVSig    the query hsv sig
     * @param comparedHSVSig the compared hsv sig
     * @return the global distance
     */
    public static float getGlobalDistance(HSVSigEntity queryHSVSig, HSVSigEntity comparedHSVSig) {
        if ((null == queryHSVSig) || (null == comparedHSVSig)) {
            return (float) 100.0;
        }

        float distance = (float) 0.0;

        distance = (float) Math.sqrt(Math.pow(queryHSVSig.getBin1() - comparedHSVSig.getBin1(),
                2) + Math.pow(queryHSVSig.getBin2() - comparedHSVSig.getBin2(),
                2) + Math.pow(queryHSVSig.getBin3()
                        - comparedHSVSig.getBin3(),
                2) + Math.pow(queryHSVSig.getBin3()
                        - comparedHSVSig.getBin3(),
                2) + Math.pow(
                queryHSVSig.getBin4()
                        - comparedHSVSig.getBin4(),
                2) + Math.pow(
                queryHSVSig.getBin5()
                        - comparedHSVSig.getBin5(),
                2) + Math.pow(
                queryHSVSig.getBin6()
                        - comparedHSVSig.getBin6(),
                2) + Math.pow(
                queryHSVSig.getBin7()
                        - comparedHSVSig.getBin7(),
                2) + Math.pow(
                queryHSVSig.getBin8()
                        - comparedHSVSig.getBin8(),
                2) + Math.pow(
                queryHSVSig.getBin9()
                        - comparedHSVSig.getBin9(),
                2) + Math.pow(
                queryHSVSig.getBin10()
                        - comparedHSVSig.getBin10(),
                2) + Math.pow(
                queryHSVSig.getBin11()
                        - comparedHSVSig.getBin11(),
                2) + Math.pow(
                queryHSVSig.getBin12()
                        - comparedHSVSig.getBin12(),
                2) + Math.pow(
                queryHSVSig.getBin13()
                        - comparedHSVSig.getBin13(),
                2) + Math.pow(
                queryHSVSig.getBin14()
                        - comparedHSVSig.getBin14(),
                2) + Math.pow(
                queryHSVSig.getBin15()
                        - comparedHSVSig.getBin15(),
                2) + Math.pow(
                queryHSVSig.getBin16()
                        - comparedHSVSig.getBin16(),
                2) + Math.pow(
                queryHSVSig.getBin17()
                        - comparedHSVSig.getBin17(),
                2) + Math.pow(
                queryHSVSig.getBin18()
                        - comparedHSVSig.getBin18(),
                2) + Math.pow(
                queryHSVSig.getBin19()
                        - comparedHSVSig.getBin19(),
                2) + Math.pow(
                queryHSVSig.getBin20()
                        - comparedHSVSig.getBin20(),
                2) + Math.pow(
                queryHSVSig.getBin21()
                        - comparedHSVSig.getBin21(),
                2) + Math.pow(
                queryHSVSig.getBin22()
                        - comparedHSVSig.getBin22(),
                2) + Math.pow(
                queryHSVSig.getBin23()
                        - comparedHSVSig.getBin23(),
                2) + Math.pow(
                queryHSVSig.getBin23()
                        - comparedHSVSig.getBin23(),
                2) + Math.pow(
                queryHSVSig.getBin24()
                        - comparedHSVSig.getBin24(),
                2));

        return distance;
    }
}