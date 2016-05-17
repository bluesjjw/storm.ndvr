/**
 * Created by jeremyjiang on 2016/5/12.
 * School of EECS, Peking University
 * Copyright (c) All Rights Reserved
 */
package cn.pku.net.db.storm.ndvr.util;

import org.apache.log4j.Logger;
import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Description:
 *
 * @author jeremyjiang
 * Created at 2016/5/12 20:40
 */

public class MyStringUtils {

    private static final Logger logger = Logger.getLogger(MyStringUtils.class);

    /**
     * Word segmentation
     *
     * @param text the text
     * @return the split text
     */
    public static List<String> wordSegment(String text) {
        List<String> splitText = new ArrayList<String>();
        StringReader sr        = new StringReader(text);
        IKSegmenter  ik        = new IKSegmenter(sr, true);
        Lexeme       lex       = null;

        try {
            while ((lex = ik.next()) != null) {
                splitText.add(lex.getLexemeText());
            }
        } catch (IOException e) {
            logger.error("IO error when use IKanalyzer. ", e);
        }

        return splitText;
    }

    public static Set<String> arrayToHashSet(String[] strArr){
        Set<String> result = new HashSet<String>();
        for (String str: strArr) {
            result.add(str);
        }
        return result;
    }
}