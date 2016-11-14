/**
 * Created by jeremyjiang on 2016/5/12.
 * School of EECS, Peking University
 * Copyright (c) All Rights Reserved
 */
package cn.pku.net.db.storm.ndvr.util;

import cn.pku.net.db.storm.ndvr.common.Const;
import cn.pku.net.db.storm.ndvr.entity.HSVSigEntity;
import cn.pku.net.db.storm.ndvr.general.TextSimilarBolt;
import org.apache.log4j.Logger;
import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

/**
 * Description:
 *
 * @author jeremyjiang
 *         Created at 2016/5/12 21:02
 */

public class SigSim {

    private static final Logger logger                  = Logger.getLogger(SigSim.class);

    /**
     * Gets global visual signature's distance (HSV)
     *
     * @param queryHSVSig    the query hsv sig
     * @param comparedHSVSig the compared hsv sig
     * @return the global distance
     */
    public static float getEuclideanDistance(HSVSigEntity queryHSVSig, HSVSigEntity comparedHSVSig) {
        if ((null == queryHSVSig) || (null == comparedHSVSig)) {
            return (float) 100.0;
        }

        float distance = (float) 0.0;

        distance = (float) Math.sqrt(Math.pow(queryHSVSig.getBin1() - comparedHSVSig.getBin1(), 2)
                + Math.pow(queryHSVSig.getBin2() - comparedHSVSig.getBin2(), 2)
                + Math.pow(queryHSVSig.getBin3() - comparedHSVSig.getBin3(), 2)
                + Math.pow(queryHSVSig.getBin3() - comparedHSVSig.getBin3(), 2)
                + Math.pow(queryHSVSig.getBin4() - comparedHSVSig.getBin4(), 2)
                + Math.pow(queryHSVSig.getBin5() - comparedHSVSig.getBin5(), 2)
                + Math.pow(queryHSVSig.getBin6() - comparedHSVSig.getBin6(), 2)
                + Math.pow(queryHSVSig.getBin7() - comparedHSVSig.getBin7(), 2)
                + Math.pow(queryHSVSig.getBin8() - comparedHSVSig.getBin8(), 2)
                + Math.pow(queryHSVSig.getBin9() - comparedHSVSig.getBin9(), 2)
                + Math.pow(queryHSVSig.getBin10() - comparedHSVSig.getBin10(), 2)
                + Math.pow(queryHSVSig.getBin11() - comparedHSVSig.getBin11(), 2)
                + Math.pow(queryHSVSig.getBin12() - comparedHSVSig.getBin12(), 2)
                + Math.pow(queryHSVSig.getBin13() - comparedHSVSig.getBin13(), 2)
                + Math.pow(queryHSVSig.getBin14() - comparedHSVSig.getBin14(), 2)
                + Math.pow(queryHSVSig.getBin15() - comparedHSVSig.getBin15(), 2)
                + Math.pow(queryHSVSig.getBin16() - comparedHSVSig.getBin16(), 2)
                + Math.pow(queryHSVSig.getBin17() - comparedHSVSig.getBin17(), 2)
                + Math.pow(queryHSVSig.getBin18() - comparedHSVSig.getBin18(), 2)
                + Math.pow(queryHSVSig.getBin19() - comparedHSVSig.getBin19(), 2)
                + Math.pow(queryHSVSig.getBin20() - comparedHSVSig.getBin20(), 2)
                + Math.pow(queryHSVSig.getBin21() - comparedHSVSig.getBin21(), 2)
                + Math.pow(queryHSVSig.getBin22() - comparedHSVSig.getBin22(), 2)
                + Math.pow(queryHSVSig.getBin23() - comparedHSVSig.getBin23(), 2)
                + Math.pow(queryHSVSig.getBin23() - comparedHSVSig.getBin23(), 2)
                + Math.pow(queryHSVSig.getBin24() - comparedHSVSig.getBin24(), 2));
        return distance;
    }

    /**
     * Gets text similarity.
     *
     * @param str1 the str 1
     * @param str2 the str 2
     * @return the text sim
     */
    public static float getTextSim(String str1, String str2) {
        List<String> str1Splits = MyStringUtils.wordSegment(str1);
        List<String> str2Splits = MyStringUtils.wordSegment(str2);
        // logger.info("Compared video title's splits: "
        // + (new Gson()).toJson(comparedTextSplits));
        if (!str2Splits.isEmpty()) {
            float str1VS2 = (float) 0.0; // query与compare逐词比较的相似度
            float str2VS1 = (float) 0.0; // compare与query逐词比较的相似度
            int sameTermNum = 0;
            // 计算query与compare相同的term数量占query总term的比例
            for (int i = 0; i < str1Splits.size(); i++) {
                int minIndex = (i - Const.STORM_CONFIG.TEXT_COMPARED_WINDOW) >= 0
                        ? (i - Const.STORM_CONFIG.TEXT_COMPARED_WINDOW) : 0;
                int maxIndex = (i
                        + Const.STORM_CONFIG.TEXT_COMPARED_WINDOW) < str2Splits
                        .size() ? (i + Const.STORM_CONFIG.TEXT_COMPARED_WINDOW)
                        : (str2Splits.size() - 1);
                for (int j = minIndex; j < maxIndex + 1; j++) {
                    if (str1Splits.get(i).equals(str2Splits.get(j))) {
                        sameTermNum++;
                        break;
                    }
                }
            }
            str1VS2 = sameTermNum / (float) str1Splits.size();

            // 计算compare与query相同的term数量占compare总term的比例
            sameTermNum = 0;
            for (int i = 0; i < str2Splits.size(); i++) {
                int minIndex = (i - Const.STORM_CONFIG.TEXT_COMPARED_WINDOW) >= 0
                        ? (i - Const.STORM_CONFIG.TEXT_COMPARED_WINDOW) : 0;
                int maxIndex = (i
                        + Const.STORM_CONFIG.TEXT_COMPARED_WINDOW) < str1Splits
                        .size() ? (i + Const.STORM_CONFIG.TEXT_COMPARED_WINDOW)
                        : (str1Splits.size() - 1);
                for (int j = minIndex; j < maxIndex + 1; j++) {
                    if (str2Splits.get(i).equals(str1Splits.get(j))) {
                        sameTermNum++;
                        break;
                    }
                }
            }
            str2VS1 = sameTermNum / (float) str2Splits.size();

            if (str2VS1 == 0 || str1VS2 == 0) {
                return (float) 0.0;
            }

            // 调和相似度
            float harmonicSimilarity = str1VS2 * str2VS1
                    / (str1VS2 + str2VS1);
            return harmonicSimilarity;
        } else {
            return (float) 0.0;
        }
    }

    /**
     * Gets split text.
     *
     * @param text the text
     * @return the split text
     */
    public static List<String> getSplitText(String text) {
        List<String> splitText = new ArrayList<String>();
        StringReader sr = new StringReader(text);
        IKSegmenter ik = new IKSegmenter(sr, true);
        Lexeme lex = null;
        try {
            while ((lex = ik.next()) != null) {
                splitText.add(lex.getLexemeText());
            }
        } catch (IOException e) {
            logger.error("IO error when use IKanalyzer. ", e);
        }
        return splitText;
    }

}