/**
 * Created by jeremyjiang on 2016/6/17.
 * School of EECS, Peking University
 * Copyright (c) All Rights Reserved
 */
package cn.pku.net.db.storm.ndvr.lsh;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * Description: Makes it easy to generate random test data sets.
 *
 * @author jeremyjiang
 *         Created at 2016/6/17 12:00
 */

public class TestUtils {

    /**
     * Generate a data set.
     * @param dimensions The number of dimensions of the vector.
     * @param datasetSize The size of the data set.
     * @param maxValue The maximum value of the coordinates of the vectors (a number between 0 and maxValue is chosen randomly).
     * @return A data set with the properties described above.
     */
    public static List<Vector> generate(int dimensions, int datasetSize, int maxValue) {
        Random rand = new Random();
        List<Vector> ret = new ArrayList<Vector>();
        for (int i = 0; i < datasetSize; i++) {
            Vector item = new Vector(dimensions);
            for (int d = 0; d < dimensions; d++) {
                double point = rand.nextInt(maxValue);
                item.set(d, point);
            }
            ret.add(item);
        }
        return ret;
    }

    /**
     * Adds neighbours to each element in a data set. Each element gets a number
     * of neighbours added. E.g. If a data set has 10 elements and 2 neighbours
     * are added, you end up with <code>10 + 2 x 10 = 30</code> elements. Each
     * element is copied and moved slightly according to radius.
     *
     * @param dataset
     *            The data set.
     * @param numberOfNeighboursToAdd
     *            The number of neighbours to add. E.g. If a data set has 10
     *            elements and 2 neighbours are added, you end up with
     *            <code>10 + 2 x 10 = 30</code> elements.
     * @param radius
     *            The radius used to create neigbours. In each dimension a
     *            random number between -radius and +radius is added to the
     *            current value.
     */
    public static void addNeighbours(List<Vector> dataset, int numberOfNeighboursToAdd,double radius){
        int datasetSize = dataset.size();
        for (int i = 0; i < datasetSize; i++) {
            Vector original = dataset.get(i);
            for (int  neighbours = 0; neighbours < numberOfNeighboursToAdd; neighbours++) {
                Vector neighbour = new Vector(original);
                neighbour.moveSlightly(radius);
                dataset.add(neighbour);
            }
        }
    }
}