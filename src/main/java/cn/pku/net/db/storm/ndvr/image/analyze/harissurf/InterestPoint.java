package cn.pku.net.db.storm.ndvr.image.analyze.harissurf;

public interface InterestPoint extends java.io.Serializable {
    public double getDistance(InterestPoint point);

    public float[] getLocation();
}
