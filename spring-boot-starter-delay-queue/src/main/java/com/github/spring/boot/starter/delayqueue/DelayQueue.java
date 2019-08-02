/*
 * ********************************************************************
 *    壹点壹滴互联(北京)教育科技有限公司         http://web.1d1d100.com/
 *
 *          Copyright ©  2018-2019.  All Rights Reserved.
 *
 *       ***************以技术引领幼教行业全面升级***************
 *       ***************用爱和责任推动幼儿教育公平***************
 *
 * ********************************************************************
 */

package com.github.spring.boot.starter.delayqueue;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.ZSetOperations;

import javax.annotation.Resource;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * 延迟队列, 支持不同业务订阅不同topic
 * <p>
 * 1.将整个Redis当做消息池，以kv形式存储消息，key为id，value为具体的消息body
 * 2.使用ZSET做优先队列，按照score维持优先级（用当前时间+需要延时的时间作为score）
 * 3.轮询ZSET，拿出score比当前时间戳大的数据（已过期的）
 * 4.根据id拿到消息池的具体消息进行消费
 * 5.消费成功，删除改队列和消息
 * 6.消费失败，让该消息重新回到队列
 *
 * @author Huzi.Wang[imhuzi.wh@gmail.com]
 * @version V1.0.0
 * @date 2019-06-05 21:41
 **/
@Slf4j
@Data
public class DelayQueue {
    /**
     * 订阅 topic
     */
    private String topic;

    /**
     * 30秒，可自己动态传入
     */
    private int delay = 30;

    /**
     * 默认100条
     */
    private int batchSize = 50;

    /**
     * 消息池前缀，以此前缀加上传递的消息id作为key，以消息{@link DelayMessage}
     * 的消息体body作为值存储
     */
    private static final String QUEUE_DELAY_POOL_PREFIX = "queue_delay:pool:%s:%s";

    /**
     * zset队列 名称 queue
     */
    private static final String QUEUE_DELAY_PREFIX = "queue_delay:queue:%s";

    /**
     * pool 延长 30分钟过期
     */
    private static final int SEMIH = 30 * 60;


    @Resource
    private RedisTemplate<String, Object> redisTemplate;

    public DelayQueue(String topic) {
        this.topic = topic;
    }

    private String poolKey(String id) {
        return String.format(QUEUE_DELAY_POOL_PREFIX, topic, id);
    }

    private String zsetKey() {
        return String.format(QUEUE_DELAY_PREFIX, topic);
    }

    /**
     * 存入消息池
     *
     * @param message
     * @return
     */
    private void addPool(DelayMessage message) {
        redisTemplate.opsForValue().set(poolKey(message.getId()), message.getBody(), message.getTtl() + SEMIH, TimeUnit.SECONDS);
    }

    /**
     * 从消息池中删除消息
     *
     * @param id
     * @return
     */
    private void delPool(String id) {
        redisTemplate.delete(poolKey(id));
    }

    /**
     * 向队列中添加消息
     *
     * @param score 优先级
     * @param val
     */
    private void addZset(long score, String val) {
        redisTemplate.opsForZSet().add(zsetKey(), val, score);
    }

    /**
     * 从队列删除消息
     *
     * @param id
     */
    private void removeFromZset(String id) {
        redisTemplate.opsForZSet().remove(zsetKey(), id);
    }

    /**
     * push 到队列
     * <p>
     * 消息id 默认 为 uuid
     * 默认延迟 30秒
     *
     * @param messageContent 消息内容
     */
    public void push(Object messageContent) {
        String seqId = RandomStringUtils;
        push(delay, seqId, messageContent);
    }

    /**
     * push到队列
     * <p>
     * 如果 传入 seqId,将会发生如下情况:
     * <p>
     * 根据seqId做唯一处理，如果延迟30秒，在 25秒的时候又有同一个seqId  push了，会更新延迟时间为30
     * <p>
     * 应用在如下场景:
     * <p>
     * 1. 离线网关监控, 比如 在 6:20 的时候 收到最后一条数据，延迟20分钟后(6:40)还未收到离消息，就作为离线逻辑处理，如果 在6:40之前收到了数据，会更新延迟时间
     * 2. 离园时间监控, 离园时间监控也是如此，直至 真正达到
     *
     * @param seqId          消息id  如果是已经存在的id，只会更新延迟时间
     * @param messageContent 消息内容
     */
    public void push(String seqId, Object messageContent) {
        push(delay, seqId, messageContent);
    }

    /**
     * push到队列
     *
     * @param delay          延迟时间
     * @param seqId          消息id
     * @param messageContent 消息内容
     */
    public void push(int delay, String seqId, Object messageContent) {
        log.info("DelayQueuePushParam:{},{},{}", seqId, delay, messageContent);
        try {
            if (messageContent != null) {
                // 将有效信息放入消息队列和消息池中
                DelayMessage message = new DelayMessage();
                // 可以添加延迟配置
                message.setDelay(delay * 1000);
                message.setCreateTime(System.currentTimeMillis());
                message.setBody(messageContent);
                message.setId(seqId);
                // 设置消息池ttl，防止长期占用 + 6分钟+30分钟
                message.setTtl(delay + 360);
                addPool(message);
                //当前时间加上延时的时间，作为score
                long delayTime = message.getCreateTime() + message.getDelay();
                addZset(delayTime, message.getId());
                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                String d = sdf.format(message.getCreateTime());
                log.info("DelayQueuePush：{},consumerTime：{}", d, sdf.format(delayTime));
            } else {
                log.info("DelayQueuePushError:消息内容为空");
            }
        } catch (Exception e) {
            log.info("DelayQueuePushError", e);
        }
    }

    /**
     * 从队列中 pop消息
     * <p>
     * batchSize default 20
     *
     * @return List<Object>
     */
    public List<Object> pop() {
        return pop(batchSize);
    }

    /**
     * 从队列中 pop消息
     *
     * @param batchSize pop 的消息数量
     * @return List<Object>
     */
    public List<Object> pop(int batchSize) {
        List<Object> data = new ArrayList<>();
        Set<ZSetOperations.TypedTuple<Object>> set = redisTemplate.opsForZSet().rangeByScoreWithScores(zsetKey(), 0, System.currentTimeMillis(), 0, batchSize);
        if (null != set) {
            long current = System.currentTimeMillis();
            for (ZSetOperations.TypedTuple<Object> item : set) {
                if (item == null) {
                    log.info("DelayQueuePopItemNull:{}", item);
                    continue;
                }
                String id = item.getValue() + "";
                Double score = item.getScore();
                if (score != null && current >= score) {
                    // 已超时的消息拿出来消费
                    Object str = "";
                    try {
                        str = redisTemplate.opsForValue().get(poolKey(id));
                        if (null != str && StringUtils.isNotBlank(str + "")) {
                            data.add(str);
                            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                            log.info("DelayQueue{}Pop:{},consumerTime：{}", topic, str, sdf.format(System.currentTimeMillis()));
                        } else {
                            // 多线程pop会出现 被其他线程先pop成功而删除
                            log.info("DelayQueue{}PopNull:{}", topic, id);
                        }
                    } catch (Exception e) {
                        //如果出了异常，则重新放回队列
                        log.warn("DelayQueuePopError，重新回到队列", e);
                        push(id, str);
                    } finally {
                        removeFromZset(id);
                        delPool(id);
                    }
                } else {
                    log.info("DelayQueuePopScoreError:{},{},{}", id, score, set);
                }
            }
        }
        return data;
    }

}
