from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.functions import MapFunction, FilterFunction
import json
import logging
import redis

logger = logging.getLogger(__name__)

class CDCChangeEventProcessor(MapFunction):
    """Process Debezium CDC change events from PostgreSQL."""
    def map(self, value):
        try:
            # Parse JSON change event
            change_event = json.loads(value)
            
            # Debezium often wraps the actual event in a 'payload' object
            if 'payload' in change_event:
                data = change_event['payload']
            else:
                data = change_event
            
            # Extract operation type
            op = data.get('op')
            op_name = {
                'c': 'INSERT',
                'u': 'UPDATE',
                'd': 'DELETE',
                'r': 'READ (Snapshot)'
            }.get(op, f'UNKNOWN ({op})')
            
            # Extract before/after data
            before = data.get('before')
            after = data.get('after')
            
            # Extract source metadata
            source = data.get('source', {})
            table = source.get('table', 'unknown')
            schema = source.get('schema', 'unknown')
            
            logger.info(f"CDC Event: {op_name} on {schema}.{table}")
            
            if op == 'c':  # INSERT
                logger.info(f"   New record: {after}")
            elif op == 'u':  # UPDATE
                logger.info(f"   Updated record:")
                logger.info(f"      Before: {before}")
                logger.info(f"      After: {after}")
            elif op == 'd':  # DELETE
                logger.info(f"   Deleted record: {before}")
            
            # Return processed event
            return json.dumps({
                'op': op,
                'op_name': op_name,
                'before': before,
                'after': after,
                'raw_event': change_event
            })
            
        except Exception as e:
            logger.error(f"Error processing CDC event: {str(e)}")
            return None

class RedisSinkMapper(MapFunction):
    def open(self, runtime_context): 
        # Cria a conexao uma unica vez por worker 
        self.r = redis.Redis(host='valkey', port=6379, db=0) 
        logger.info("Conexao Redis aberta.") 
    
    def map(self, value): 
        event_data = json.loads(value) 
        op = event_data.get('op') 
        after = event_data.get('after') 
        before = event_data.get('before') 
        
        # Usa a conexao persistente self.r 
        # Usa a estrutura Stream do Redis: https://redis.io/docs/latest/develop/data-types/streams/ 
        if op == 'c': 
            id = after.get('id') 
            name = after.get('name') 
            res = self.r.xadd("cdc_redis_stream", {"id": id, "name": name, "operation" : "INSERT"}) 

        elif op == 'u': 
            id = after.get('id') 
            name = after.get('name') 
            old_name = before.get('name') 
            res = self.r.xadd("cdc_redis_stream", {"id": id, "name": name, "old_name": old_name, "operation" : "UPDATE"}) 
        
        elif op == 'd': 
            id = before.get('id') 
            res = self.r.xadd("cdc_redis_stream", {"id": id, "operation": "DELETE"}) 
            
        return value 
    
    def close(self): # Fecha a conexao ao encerrar a task 
        if hasattr(self, 'r'): 
            self.r.close() 
            logger.info("Conexao Redis fechada.") 
            
"""Create and configure the Flink CDC job.""" 
def create_flink_cdc_job():
    env = StreamExecutionEnvironment.get_execution_environment() 
    env.set_parallelism(1) 

    # Kafka consumer configuration 
    kafka_consumer = FlinkKafkaConsumer( 
        topics="postgres.bigdata.cdc.public.teste", ## defina com o nome do topico correto 
        deserialization_schema=SimpleStringSchema(), 
        properties={ 
            'bootstrap.servers': 'kafka:9092',
            'group.id': 'flink-cdc-consumer', 
            'auto.offset.reset': 'earliest', 
            'enable.auto.commit': 'true' 
            } 
        ) 

    # Build processing pipeline 
    cdc_stream = env.add_source(kafka_consumer) 
    processed_stream = cdc_stream.map(CDCChangeEventProcessor()) 
    filtered_stream = processed_stream.filter(lambda x: x is not None) 

    # Apply sinks to stream 
    filtered_stream.map(RedisSinkMapper()) 

    return env 

# Execute the Flink CDC job
create_flink_cdc_job().execute("Flink CDC Job")