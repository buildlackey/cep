import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import com.google.gson.Gson;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class JsonHelper implements Serializable {

    public static String toJson(Tuple input) {
        Fields fields = input.getFields();
        List<String> fieldNames = fields.toList();

        Map<String, Object> tupleAsMap = new HashMap<String, Object>();
        for (String fieldName : fieldNames) {
            tupleAsMap.put(fieldName, input.getValueByField(fieldName));
        }

        String json = new Gson().toJson(tupleAsMap);
        System.out.println("====++++++++++++++++++++++++++::>  tuple as Json:" + json);
        return json;
    }
}