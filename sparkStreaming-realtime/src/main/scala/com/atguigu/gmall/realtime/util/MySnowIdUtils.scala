package com.atguigu.gmall.realtime.util

case class MySnowIdUtils(val myWorkerId: Long) {
  private val START_TIMESTAMP = 1577808000000L // 2020-01-01 00:00:00

  // 机器ID所占的位数
  private val WORKER_ID_BITS = 10L
  // 序列号所占的位数
  private val SEQUENCE_BITS = 12L
  // 机器ID的最大值
  private val MAX_WORKER_ID: Long = ~(-(1L) << WORKER_ID_BITS)
  // 序列号的最大值
  private val MAX_SEQUENCE: Long = ~(-(1L) << SEQUENCE_BITS)
  // 机器ID向左移12位  (12位序列号)
  private var WORKER_ID_SHIFT: Long = SEQUENCE_BITS
  // 时间戳向左移22位  (12位序列号+10位机器ID)
  private var TIMESTAMP_SHIFT: Long = SEQUENCE_BITS + WORKER_ID_BITS
  // 上一次生成ID的时间戳
  private var lastTimestamp: Long = -1L
  // 当前毫秒内生成的序列号
  private var sequence = 0L
  // 机器ID (0~1023)
  private var workerId: Long = myWorkerId

  if (myWorkerId < 0 || myWorkerId > MAX_WORKER_ID) throw new IllegalArgumentException("workerId error")

  def nextId: Long = {
    var timestamp: Long = System.currentTimeMillis
    // 如果当前时间小于上一次ID生成的时间戳，说明系统时间回退过，抛出异常
    if (timestamp < lastTimestamp) throw new RuntimeException("Clock moved backwards. Refusing to generate id")
    // 如果是同一时间生成的ID（同一毫秒内），则进行序列号自增
    if (lastTimestamp == timestamp) {
      sequence = (sequence + 1) & MAX_SEQUENCE
      // 当前毫秒内的序列号已经用完，等待下一毫秒再生成
      if (sequence == 0) timestamp = tilNextMillis(lastTimestamp)
    }
    else { // 不同时间生成ID，序列号重置为0
      sequence = 0L
    }
    // 记录生成的ID时间戳，用于下次生成ID时比较
    lastTimestamp = timestamp
    // 生成唯一ID
    ((timestamp - START_TIMESTAMP) << TIMESTAMP_SHIFT) | (workerId << WORKER_ID_SHIFT) | sequence
  }

  /**
   * 等待下一个毫秒
   *
   * @param lastTimestamp 上一次生成ID的时间戳
   * @return 当前时间戳
   */
  private def tilNextMillis(lastTimestamp: Long): Long = {
    var timestamp: Long = System.currentTimeMillis
    while ( {
      timestamp <= lastTimestamp
    }) timestamp = System.currentTimeMillis
    timestamp
  }
}
