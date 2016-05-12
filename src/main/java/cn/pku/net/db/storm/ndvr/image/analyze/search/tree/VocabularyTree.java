package cn.pku.net.db.storm.ndvr.image.analyze.search.tree;

import java.util.List;

import cn.pku.net.db.storm.ndvr.image.analyze.search.cluster.Clusterable;

public interface VocabularyTree {
    public List<Float> getCurrentWords();

    public List<Integer> addImage(List<? extends Clusterable> imagePoint);

    public void reset();
}
