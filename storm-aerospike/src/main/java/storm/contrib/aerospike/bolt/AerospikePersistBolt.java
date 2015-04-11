package storm.contrib.aerospike.bolt;
import java.util.Map;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.policy.RecordExistsAction;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Fields;
import twitter4j.Status;


public class AerospikePersistBolt extends BaseRichBolt {
	private OutputCollector collector;

	private AerospikeClient AerospikeClient;
	private WritePolicy AerospikeWritePolicy;
	private final String aerospikeHost;
	private final int aerospikePort;
	private final String aerospikeNamespace;
	private final String aerospikeSet;
	private String AerospikeKeyName;


	public AerospikePersistBolt(String AerospikeHost, int AerospikePort, String AerospikeNamespace, String AerospikeSet) {
		this.aerospikeHost = AerospikeHost;
		this.aerospikePort = AerospikePort;
		this.aerospikeNamespace = AerospikeNamespace;
		this.aerospikeSet = AerospikeSet;
	}


	public AerospikePersistBolt(String AerospikeHost, int AerospikePort, String AerospikeNamespace, String AerospikeSet, String AerospikeKeyName) {
		this.aerospikeHost = AerospikeHost;
		this.aerospikePort = AerospikePort;
		this.aerospikeNamespace = AerospikeNamespace;
		this.aerospikeSet = AerospikeSet;
		this.AerospikeKeyName = AerospikeKeyName;
	}

	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {

		this.collector = collector;
		try {
			this.AerospikeClient = new AerospikeClient(this.aerospikeHost, this.aerospikePort);
			this.AerospikeWritePolicy = new WritePolicy();
			this.AerospikeWritePolicy.maxRetries=10;
			this.AerospikeWritePolicy.recordExistsAction = RecordExistsAction.UPDATE;
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	/*
 	 * I am only accepting an int or string as the key value for now
 	 */
	public void execute(Tuple input) {
/*
	     if (true) {
		if ((counter++ % 100000) == 0) {
			System.out.println("records received: " + counter);
		}
		this.collector.ack(input);
		return;
	     }
*/
	     // First we need to get our key value

		Status status = (Status) input.getValueByField("tweet");
		System.out.println("---------------------------------");
		System.out.println(status.getId());
		System.out.println(status.getUser().getName());
		Key key = new Key("test","stormset1", status.getId());
		Bin bin1 = new Bin("Username", status.getUser().getName());
		this.AerospikeClient.put(this.AerospikeWritePolicy, key, bin1);
		System.out.println("sd");
//	     if (!input.contains(AerospikeKeyName)) {
//		// Need to raise an exception since the KeyName isn't in the Tuple
//		throw new RuntimeException();
//	     }
//
//	     try {
//		     inputField = input.getValueByField(this.AerospikeKeyName);
//		     AerospikeKey = new Key(this.aerospikeNamespace, this.AerospikeSet, GetAerospikeValue(inputField).toString());
//		     // System.out.println("Key : " + input.getValueByField(this.AerospikeKeyName));
//	     } catch (AerospikeException asE) {
//	     		throw new RuntimeException(asE);
//	     }
//
//             for (String field : input.getFields()) {
//		if (field.equals(AerospikeKeyName)) {
//			continue; // Skip the key value. Maybe this should be optional
//		}
//	        inputField = input.getValueByField(field);
//		AerospikeBins[BinIndex++] = new Bin(field, GetAerospikeValue(inputField));
//	     }
//	     // Now its time to create/update the record in the Aerospike DB
//	     try {
//		     AerospikeClient.put(this.AerospikeWritePolicy, AerospikeKey, AerospikeBins);
//	     } catch (AerospikeException asE) {
//	     		throw new RuntimeException(asE);
//	     }
//	     this.collector.emit(input, new Values(input.getValueByField(this.AerospikeKeyName)));
//	     this.collector.ack(input);

	}

	private Object GetAerospikeValue(Object KeyValue) {
		return (KeyValue);
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer){
		declarer.declare(new Fields(this.AerospikeKeyName));
	}


	public void cleanup() {
		AerospikeClient.close();
	}
}

