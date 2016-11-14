/**
 * Created by jeremyjiang on 2016/6/17.
 * School of EECS, Peking University
 * Copyright (c) All Rights Reserved
 */
package cn.pku.net.db.storm.ndvr.lsh;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Random;

/**
 * Description: An Vector contains a vector of 'dimension' values. It serves as the main data
 * structure that is stored and retrieved. It also has an identifier (key).
 *
 * @author jeremyjiang
 * Created at 2016/6/17 10:34
 */

public class Vector implements Serializable {

    private static final long serialVersionUID = 5169504339456492327L;

    /**
     * Values are stored here.
     */
    private double[] values;


    /**
     * An optional key, identifier for the vector.
     */
    private String key;

    /**
     * Creates a new vector with the requested number of dimensions.
     * @param dimensions The number of dimensions.
     */
    public Vector(int dimensions) {
        this(null,new double[dimensions]);
    }


    /**
     * Copy constructor.
     * @param other The other vector.
     */
    public Vector(Vector other){
        //copy the values
        this(other.getKey(), Arrays.copyOf(other.values, other.values.length));
    }

    /**
     * Creates a vector with the values and a key
     * @param key The key of the vector.
     * @param values The values of the vector.
     */
    public Vector(String key,double[] values){
        this.values = values;
        this.key = key;
    }

    /**
     * Moves the vector slightly, adds a value selected from -radius to +radius to each element.
     * @param radius The radius determines the amount to change the vector.
     */
    public void moveSlightly(double radius){
        Random rand = new Random();
        for (int d = 0; d < getDimensions(); d++) {
            //copy the point but add or subtract a value between -radius and +radius
            double diff = radius + (-radius - radius) * rand.nextDouble();
            double point = get(d) + diff;
            set(d, point);
        }
    }



    /**
     * Set a value at a certain dimension d.
     * @param dimension The dimension, index for the value.
     * @param value The value to set.
     */
    public void set(int dimension, double value) {
        values[dimension] = value;
    }

    /**
     * Returns the value at the requested dimension.
     * @param dimension The dimension, index for the value.
     * @return Returns the value at the requested dimension.
     */
    public double get(int dimension) {
        return values[dimension];
    }

    /**
     * @return The number of dimensions this vector has.
     */
    public int getDimensions(){
        return values.length;
    }

    /**
     * Calculates the dot product, or scalar product, of this vector with the
     * other vector.
     *
     * @param other
     *            The other vector, should have the same number of dimensions.
     * @return The dot product of this vector with the other vector.
     * @exception ArrayIndexOutOfBoundsException
     *                when the two vectors do not have the same dimensions.
     */
    public double dot(Vector other) {
        double sum = 0.0;
        for(int i=0; i < getDimensions(); i++) {
            sum += values[i] * other.values[i];
        }
        return sum;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getKey() {
        return key;
    }

    public String toString(){
        StringBuilder sb= new StringBuilder();
        sb.append("values:[");
        for(int d=0; d < getDimensions() - 1; d++) {
            sb.append(values[d]).append(",");
        }
        sb.append(values[getDimensions()-1]).append("]");
        return sb.toString();
    }
}