package com.github.jmabuin.metacachespark;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.serializers.MapSerializer;
import com.github.jmabuin.metacachespark.database.LocationBasicComparator;
import com.github.jmabuin.metacachespark.database.MatchCandidate;
import org.apache.spark.serializer.KryoRegistrator;

import java.io.Serializable;
import java.util.List;
import java.util.TreeMap;

/**
 * Created by chema on 6/20/17.
 */
public class MyKryoRegistrator implements KryoRegistrator, Serializable {

    @Override
    public void registerClasses(Kryo kryo) {
        // Product POJO associated to a product Row from the DataFrame
        //kryo.register(MyRecord.class);
        MapSerializer serializer = new MapSerializer();
        kryo.register(TreeMap.class, serializer);
        serializer.setKeyClass(LocationBasic.class, kryo.getSerializer(LocationBasic.class));
        serializer.setValueClass(Integer.class, kryo.getSerializer(Integer.class));
        serializer.setKeysCanBeNull(false);
        //serializer.setKeyClass(String.class, kryo.getSerializer(String.class));
        kryo.register(Location.class);
        kryo.register(Sketch.class);
        kryo.register(LocationBasic.class);
        kryo.register(Locations.class);
        kryo.register(Integer.class);
        kryo.register(LocationBasicComparator.class);
        kryo.register(MatchCandidate.class);
        kryo.register(List.class);

                /*
                Location.class,
					Sketch.class,
					LocationBasic.class,
					TreeMap.class,
					proba.getClass(),
					LocationBasicComparator.class,
					Integer.class};
                 */

    }



}
