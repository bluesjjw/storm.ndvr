/**
 * Created by jeremyjiang on 2016/5/12.
 * School of EECS, Peking University
 * Copyright (c) All Rights Reserved
 */
package cn.pku.net.db.storm.ndvr.util;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;

/**
 * Description:
 *
 * @author jeremyjiang
 * Created at 2016/5/12 20:40
 */

public class TextUtils {

    /**
     * Word segmentation
     *
     * @param text the text
     * @return the split text
     */
    public static List<String> getSplitText(String text) {
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
}