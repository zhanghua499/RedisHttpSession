package zh.redis;

import java.util.ArrayList;
import java.util.List;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.exceptions.JedisConnectionException;

public abstract class RedisQueueProductTemplate<T> {
	
	protected String queue_name;
	
	//是否需要拼接字符串
	protected boolean needBuild = false;
		
	//拼接字符串限制数
	protected int limitBuild = 150;
	
	public RedisQueueProductTemplate(String queue_name){
		this.queue_name = queue_name;
	}
	
	public RedisQueueProductTemplate(String queue_name,boolean needBuild){
		this.queue_name = queue_name;
		this.needBuild = needBuild;
	}
	
	public RedisQueueProductTemplate(String queue_name,boolean needBuild,int limitBuild){
		this.queue_name = queue_name;
		this.needBuild = needBuild;
		this.limitBuild = limitBuild;
	}
	
	public void product(){
		RedisClient client = this.getRedisClient();
		Jedis jedis = null;
		Pipeline p = null;
		List<T> list = new ArrayList<T>();
		while(true){
			try{
				jedis = client.getRedis();
				p = jedis.pipelined();
				Long length = jedis.llen(queue_name);
				if(length>0){
					Thread.sleep(1000);
					continue;
				}
				list = this.getPushList();
				
				if(list.size() == 0)
					break;
				else{
					if(needBuild){
						callback(jedis,p,buildList(list));
					}
					else{
						for(T entry:list){
							try{
								callback(jedis,p,entry);
							}
							catch(JedisConnectionException e){
								if (jedis != null) 
									client.returnBrokeRedis(jedis);
								jedis = client.getRedis();
								p = jedis.pipelined();
								e.printStackTrace();
							}
						}
					}
					p.sync();
				}
			}
			catch(JedisConnectionException e){
				if (jedis != null) 
					client.returnBrokeRedis(jedis);
				jedis = client.getRedis();
				p = jedis.pipelined();
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
	
	private List<String> buildList(List<T> list){
		StringBuilder sb = new StringBuilder();
		List<String> rslist = new ArrayList<String>();
		int i=0;
		for(T map : list){
			try{
				
				String keyword = getElement(map);
				if(i<limitBuild){
					if(sb.toString().equals(""))
						sb.append(keyword);
					else
						sb.append(","+keyword);
					i++;
				}else{
					rslist.add(sb.toString());
					sb = new StringBuilder();
					sb.append(keyword);
					i=1;
				}
			}

			catch(Exception e){
				e.printStackTrace();
			}
		}
		rslist.add(sb.toString());
		return rslist;
	}

	protected abstract RedisClient getRedisClient();
	
	protected abstract List<T> getPushList();
	
	protected abstract void callback(Jedis jedis,Pipeline pipeLine,Object params);
	
	protected abstract String getElement(T param);
}
