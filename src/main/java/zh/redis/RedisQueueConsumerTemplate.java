package zh.redis;

import java.util.List;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisConnectionException;

public abstract class RedisQueueConsumerTemplate {
	
	protected String queue_name;

	public RedisQueueConsumerTemplate(String queue_name){
		this.queue_name = queue_name;
	}
	
	public void consumer(){
		RedisClient client = getRedisClient();
		Jedis jedis = null;
		while(true){
			try{
				jedis = client.getRedis();
				List<String> list = jedis.brpop(0, this.queue_name);
				if(list.size()>1){
					String value  = list.get(1);
					this.callback(value);
				}
				
			}
			catch(JedisConnectionException e){
				if (jedis != null) 
					client.returnBrokeRedis(jedis);
				jedis = client.getRedis();
				e.printStackTrace();
			}
			catch(Exception e){
				e.printStackTrace();
			}
			finally{
				if (jedis != null) {
					client.returnRedis(jedis);
				}
			}
		}	
	}
	
	protected abstract RedisClient getRedisClient();
	
	protected abstract void callback(String value);
	
	

}
