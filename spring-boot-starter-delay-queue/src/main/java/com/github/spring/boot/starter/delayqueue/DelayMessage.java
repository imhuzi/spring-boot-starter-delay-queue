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

/**
 * @author Huzi.Wang[imhuzi.wh@gmail.com]
 * @version V1.0.0
 * @date 2019-06-13 14:51
 **/

@Data
public class DelayMessage {
    /**
     * 消息id
     */
    private String id;
    /**
     * 消息延迟/毫秒
     */
    private long delay;

    /**
     * 消息存活时间
     */
    private int ttl;
    /**
     * 消息体，对应业务内容
     */
    private Object body;
    /**
     * 创建时间，如果只有优先级没有延迟，可以设置创建时间为0
     * 用来消除时间的影响
     */
    private long createTime;
}
