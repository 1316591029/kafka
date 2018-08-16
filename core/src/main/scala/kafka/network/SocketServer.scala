/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.network

import java.io.IOException
import java.net._
import java.nio.channels._
import java.nio.channels.{Selector => NSelector}
import java.util
import java.util.concurrent._
import java.util.concurrent.atomic._

import com.yammer.metrics.core.Gauge
import kafka.cluster.{BrokerEndPoint, EndPoint}
import kafka.common.KafkaException
import kafka.metrics.KafkaMetricsGroup
import kafka.server.KafkaConfig
import kafka.utils._
import org.apache.kafka.common.errors.InvalidRequestException
import org.apache.kafka.common.metrics._
import org.apache.kafka.common.network.{ChannelBuilders, KafkaChannel, LoginType, Mode, Selectable, Selector => KSelector}
import org.apache.kafka.common.security.auth.KafkaPrincipal
import org.apache.kafka.common.protocol.SecurityProtocol
import org.apache.kafka.common.protocol.types.SchemaException
import org.apache.kafka.common.utils.{Time, Utils}

import scala.collection._
import JavaConverters._
import scala.util.control.{ControlThrowable, NonFatal}

/**
 * An NIO socket server. The threading model is
 *   1 Acceptor thread that handles new connections
 *   Acceptor has N Processor threads that each have their own selector and read requests from sockets
 *   M Handler threads that handle requests and produce responses back to the processor threads for writing.
 */
class SocketServer(val config: KafkaConfig, val metrics: Metrics, val time: Time) extends Logging with KafkaMetricsGroup {

  /*
   * Endpoint 集合。一般的服务器都有多块网卡，可以配置多个 IP，Kafka 可以同时监听多个端口。
   * Endpoint 类中封装了需要监听的 host、port 及使用的网络协议。每个 Endpoint 都会创建一个
   * 对应的 Acceptor 对象
   */
  private val endpoints = config.listeners
  // Processor 线程个数
  private val numProcessorThreads = config.numNetworkThreads
  //  在 RequestChannel 的 requestQueue 中缓存的最大请求个数
  private val maxQueuedRequests = config.queuedMaxRequests
  // Processor 线程的总个数，即 numProcessorThread * endpoints.size
  private val totalProcessorThreads = numProcessorThreads * endpoints.size

  // 每个 IP 上能创建的最大连接数
  private val maxConnectionsPerIp = config.maxConnectionsPerIp
  // Map[String, Int] 类型，具体制定某 IP 上最大的连接数，这里制定的最大连接数会覆盖上面的 maxConnectionsPerIp 字段的值
  private val maxConnectionsPerIpOverrides = config.maxConnectionsPerIpOverrides

  this.logIdent = "[Socket Server on Broker " + config.brokerId + "], "

  // Processor 线程与 Handler 线程之间交互数据的队列
  // 创建 RequestChannel，其中有 totalProcessorThreads 个 responseQueue 队列
  val requestChannel = new RequestChannel(totalProcessorThreads, maxQueuedRequests)

  // Processor 线程的集合。此集合中包含所有 Endpoint 对应的 Processors 线程
  // 创建保存 Processor 的数组
  private val processors = new Array[Processor](totalProcessorThreads)

  // 创建保存 Acceptor 的集合
  private[network] val acceptors = mutable.Map[EndPoint, Acceptor]()
  /*
   * ConnectionQuotas 类型的对象。在 ConnectionQuotas 中，提供了控制每个 IP 上的
   * 最大连接数的功能。底层通过一个 Map 对象，记录每个 IP 地址上建立的连接数，创建
   * 新 Connect 时与 maxConnectionsPerIpOverrides 指定的最大值（或 maxConnectionsPerIp）
   * 进行笔记，若超出限制，则报错。因为有多个 Acceptor 线程并发访问底层的 Map 对象，则
   * 需要 synchronized 进行同步
   */
  private var connectionQuotas: ConnectionQuotas = _

  private val allMetricNames = (0 until totalProcessorThreads).map { i =>
    val tags = new util.HashMap[String, String]()
    tags.put("networkProcessor", i.toString)
    metrics.metricName("io-wait-ratio", "socket-server-metrics", tags)
  }

  /**
   * Start the socket server
   * 初始化核心方法
   */
  def startup() {
    this.synchronized { // 同步

      // 创建 ConnectionQuotas
      connectionQuotas = new ConnectionQuotas(maxConnectionsPerIp, maxConnectionsPerIpOverrides)

      // Socket 的 sendBuffer 大小
      val sendBufferSize = config.socketSendBufferBytes
      // Socket 的 receiveBuffer 大小
      val recvBufferSize = config.socketReceiveBufferBytes
      val brokerId = config.brokerId

      var processorBeginIndex = 0

      endpoints.values.foreach { endpoint => // 遍历 Endpoints 集合
        val protocol = endpoint.protocolType
        val processorEndIndex = processorBeginIndex + numProcessorThreads

        // processors 数组从 processorBeginIndex~processorEndIndex，都是当前 Endpoint 对应的 Processor 对象的集合
        for (i <- processorBeginIndex until processorEndIndex)
          // 创建 Processor
          processors(i) = newProcessor(i, connectionQuotas, protocol)

        // 创建 Acceptor，同事为 processor 创建对应的线程
        val acceptor = new Acceptor(endpoint, sendBufferSize, recvBufferSize, brokerId,
          processors.slice(processorBeginIndex, processorEndIndex) /*指定了 processors 数组重在此 Acceptor 对象对应的 Processor 对象*/,
          connectionQuotas)
        acceptors.put(endpoint, acceptor)
        // 创建 Acceptor 对应的线程，并启动
        Utils.newThread("kafka-socket-acceptor-%s-%d".format(protocol.toString, endpoint.port), acceptor, false).start()
        // 主线程阻塞等待 Acceptor 线程启动完成
        acceptor.awaitStartup()

        // 修改 processorBeginIndex，为下一个 Endpoint 准备
        processorBeginIndex = processorEndIndex
      }
    }

    newGauge("NetworkProcessorAvgIdlePercent",
      new Gauge[Double] {
        def value = allMetricNames.map( metricName =>
          metrics.metrics().get(metricName).value()).sum / totalProcessorThreads
      }
    )

    info("Started " + acceptors.size + " acceptor threads")
  }

  // register the processor threads for notification of responses
  /*
   * 向 RequestChannel 中添加一个监听器。此监听器实现的功能是：当 Handler 线程向某个
   * responseQueue 中写入数据时，会唤醒对应的 Processor 线程进行处理
   */
  requestChannel.addResponseListener(id => processors(id).wakeup())

  /**
   * Shutdown the socket server
   */
  def shutdown() = {
    info("Shutting down")
    this.synchronized {
      acceptors.values.foreach(_.shutdown) // 调用所有 Acceptor 的 shutdown
      processors.foreach(_.shutdown) // 调用所有 Processor 的 shutdown
    }
    info("Shutdown completed")
  }

  def boundPort(protocol: SecurityProtocol = SecurityProtocol.PLAINTEXT): Int = {
    try {
      acceptors(endpoints(protocol)).serverChannel.socket().getLocalPort
    } catch {
      case e: Exception => throw new KafkaException("Tried to check server's port before server was started or checked for port of non-existing protocol", e)
    }
  }

  /* `protected` for test usage */
  protected[network] def newProcessor(id: Int, connectionQuotas: ConnectionQuotas, protocol: SecurityProtocol): Processor = {
    new Processor(id,
      time,
      config.socketRequestMaxBytes,
      requestChannel,
      connectionQuotas,
      config.connectionsMaxIdleMs,
      protocol,
      config.values,
      metrics
    )
  }

  /* For test usage */
  private[network] def connectionCount(address: InetAddress): Int =
    Option(connectionQuotas).fold(0)(_.get(address))

  /* For test usage */
  private[network] def processor(index: Int): Processor = processors(index)

}

/**
 * A base class with some helper variables and methods
 * @param connectionQuotas 在 close() 方法中，根据传入的 ConnectionId，关闭
  *                         SocketChannel 并减少 ConnectionQuotas 中记录的
  *                         连接数
 */
private[kafka] abstract class AbstractServerThread(connectionQuotas: ConnectionQuotas) extends Runnable with Logging {

  // count 为 1 的 CountDownLatch 对象，标识了当前线程的 startup 操作是否完成
  private val startupLatch = new CountDownLatch(1)
  // count 为 1 的 CountDownLatch 对象，标识了当前线程的 shutdown 操作是否完成
  private val shutdownLatch = new CountDownLatch(1)
  // 标识当前线程是否存活，在初始化时设置为 true，在 shutdown() 方法中会将 alive 设置为 false
  private val alive = new AtomicBoolean(true)

  // 抽象方法，由子类实现
  def wakeup()

  /**
   * Initiates a graceful shutdown by signaling to stop and waiting for the shutdown to complete
   */
  def shutdown(): Unit = {
    alive.set(false) // 修改运行状态
    wakeup() // 唤醒当前 AbstractServerThread
    shutdownLatch.await() // 阻塞等待关闭操作完成
  }

  /**
   * Wait for the thread to completely start up
   */
  def awaitStartup(): Unit = startupLatch.await

  /**
   * Record that the thread startup is complete
   */
  protected def startupComplete() = {
    startupLatch.countDown()
  }

  /**
   * Record that the thread shutdown is complete
   */
  protected def shutdownComplete() = shutdownLatch.countDown()

  /**
   * Is the server still running?
   */
  protected def isRunning = alive.get

  /**
   * Close the connection identified by `connectionId` and decrement the connection count.
   * 关闭指定连接
   */
  def close(selector: KSelector, connectionId: String) {
    val channel = selector.channel(connectionId)
    if (channel != null) {
      debug(s"Closing selector connection $connectionId")
      val address = channel.socketAddress
      if (address != null)
        connectionQuotas.dec(address) // 修改连接数
      selector.close(connectionId) // 关闭连接
    }
  }

  /**
   * Close `channel` and decrement the connection count.
   */
  def close(channel: SocketChannel) {
    if (channel != null) {
      debug("Closing connection from " + channel.socket.getRemoteSocketAddress())
      connectionQuotas.dec(channel.socket.getInetAddress)
      swallowError(channel.socket().close())
      swallowError(channel.close())
    }
  }
}

/**
 * Thread that accepts and configures new connections. There is one of these per endpoint.
 */
private[kafka] class Acceptor(val endPoint: EndPoint,
                              val sendBufferSize: Int,
                              val recvBufferSize: Int,
                              brokerId: Int,
                              processors: Array[Processor],
                              connectionQuotas: ConnectionQuotas) extends AbstractServerThread(connectionQuotas) with KafkaMetricsGroup {

  // 创建 nioSelector
  private val nioSelector = NSelector.open()
  // 创建 ServerSocketChannel
  val serverChannel = openServerSocket(endPoint.host, endPoint.port)

  this.synchronized {
    // 为其对应的每个 Processor 都创建对应的线程并启动
    processors.foreach { processor =>
      Utils.newThread("kafka-network-thread-%d-%s-%d"
        .format(brokerId, endPoint.protocolType.toString, processor.id), processor, false)
        .start()
    }
  }

  /**
   * Accept loop that checks for new connection attempts
   */
  def run() {
    // 注册 OP_ACCEPT 事件
    serverChannel.register(nioSelector, SelectionKey.OP_ACCEPT)
    // 标识当前线程启动操作已经完成
    startupComplete()
    try {
      var currentProcessor = 0
      while (isRunning) { // 检测线程运行状态
        try {
          val ready = nioSelector.select(500) // 等待关注的事件
          if (ready > 0) {
            val keys = nioSelector.selectedKeys()
            val iter = keys.iterator()
            while (iter.hasNext && isRunning) {
              try {
                val key = iter.next
                iter.remove()
                if (key.isAcceptable) // 调用 accept() 方法处理 OP_ACCEPT 事件
                  accept(key, processors(currentProcessor))
                else // 若不是 OP_ACCEPT 事件，报错
                  throw new IllegalStateException("Unrecognized key state for acceptor thread.")

                // round robin to the next processor thread
                // 更新 currentProcessor，从这里看出，使用 Round-Robin 的方式选择 Processor
                currentProcessor = (currentProcessor + 1) % processors.length
              } catch {
                case e: Throwable => error("Error while accepting connection", e)
              }
            }
          }
        }
        catch {
          // We catch all the throwables to prevent the acceptor thread from exiting on exceptions due
          // to a select operation on a specific channel or a bad request. We don't want
          // the broker to stop responding to requests from other clients in these scenarios.
          case e: ControlThrowable => throw e
          case e: Throwable => error("Error occurred", e)
        }
      }
    } finally {
      debug("Closing server socket and selector.")
      swallowError(serverChannel.close())
      swallowError(nioSelector.close())
      shutdownComplete() // 标识此线程的关闭操作已完成
    }
  }

  /*
   * Create a server socket to listen for connections on.
   */
  private def openServerSocket(host: String, port: Int): ServerSocketChannel = {
    val socketAddress =
      if(host == null || host.trim.isEmpty)
        new InetSocketAddress(port)
      else
        new InetSocketAddress(host, port)
    val serverChannel = ServerSocketChannel.open()
    serverChannel.configureBlocking(false)
    if (recvBufferSize != Selectable.USE_DEFAULT_BUFFER_SIZE)
      serverChannel.socket().setReceiveBufferSize(recvBufferSize)

    try {
      serverChannel.socket.bind(socketAddress)
      info("Awaiting socket connections on %s:%d.".format(socketAddress.getHostString, serverChannel.socket.getLocalPort))
    } catch {
      case e: SocketException =>
        throw new KafkaException("Socket server failed to bind to %s:%d: %s.".format(socketAddress.getHostString, port, e.getMessage), e)
    }
    serverChannel
  }

  /*
   * Accept a new connection
   */
  def accept(key: SelectionKey, processor: Processor) {
    val serverSocketChannel = key.channel().asInstanceOf[ServerSocketChannel]
    // 创建 socketChannel
    val socketChannel = serverSocketChannel.accept()
    try {
      // 增加 ConnectionQuotas 中记录的连接数
      connectionQuotas.inc(socketChannel.socket().getInetAddress)

      // 配置 SocketChannel 的相关属性，例如 sendBufferSize、keepalive 等
      socketChannel.configureBlocking(false)
      socketChannel.socket().setTcpNoDelay(true)
      socketChannel.socket().setKeepAlive(true)
      if (sendBufferSize != Selectable.USE_DEFAULT_BUFFER_SIZE)
        socketChannel.socket().setSendBufferSize(sendBufferSize)

      debug("Accepted connection from %s on %s and assigned it to processor %d, sendBufferSize [actual|requested]: [%d|%d] recvBufferSize [actual|requested]: [%d|%d]"
            .format(socketChannel.socket.getRemoteSocketAddress, socketChannel.socket.getLocalSocketAddress, processor.id,
                  socketChannel.socket.getSendBufferSize, sendBufferSize,
                  socketChannel.socket.getReceiveBufferSize, recvBufferSize))

      // 将 SocketChannel 交给 Processors 处理
      processor.accept(socketChannel)
    } catch {
      case e: TooManyConnectionsException =>
        info("Rejected connection from %s, address already has the configured maximum of %d connections.".format(e.ip, e.count))
        close(socketChannel)
    }
  }

  /**
   * Wakeup the thread for selection.
   */
  @Override
  def wakeup = nioSelector.wakeup()

}

/**
 * Thread that processes all requests from a single connection. There are N of these running in parallel
 * each of which has its own selector
 */
private[kafka] class Processor(val id: Int,
                               time: Time,
                               maxRequestSize: Int,
                               requestChannel: RequestChannel, // Processor 与 Handler 线程之间传递数据的队列
                               connectionQuotas: ConnectionQuotas,
                               connectionsMaxIdleMs: Long,
                               protocol: SecurityProtocol,
                               channelConfigs: java.util.Map[String, _],
                               metrics: Metrics) extends AbstractServerThread(connectionQuotas) with KafkaMetricsGroup {

  private object ConnectionId {
    def fromString(s: String): Option[ConnectionId] = s.split("-") match {
      case Array(local, remote) => BrokerEndPoint.parseHostPort(local).flatMap { case (localHost, localPort) =>
        BrokerEndPoint.parseHostPort(remote).map { case (remoteHost, remotePort) =>
          ConnectionId(localHost, localPort, remoteHost, remotePort)
        }
      }
      case _ => None
    }
  }

  private case class ConnectionId(localHost: String, localPort: Int, remoteHost: String, remotePort: Int) {
    override def toString: String = s"$localHost:$localPort-$remoteHost:$remotePort"
  }

  // 保存由此 Processor 处理的新建的 SocketChannel
  private val newConnections = new ConcurrentLinkedQueue[SocketChannel]()
  // 保存未发送的响应。客户端中 InFlightRequests 区别是，客户端并不会对服务端发送
  // 的响应消息再次发送确认，所以 inflightResponse 中的响应会在发送成功后移除，而
  // InFlightRequests 中的请求是在收到响应后才移除。
  private val inflightResponses = mutable.Map[String, RequestChannel.Response]()
  private val metricTags = Map("networkProcessor" -> id.toString).asJava

  newGauge("IdlePercent",
    new Gauge[Double] {
      def value = {
        metrics.metrics().get(metrics.metricName("io-wait-ratio", "socket-server-metrics", metricTags)).value()
      }
    },
    metricTags.asScala
  )

  // 负责管理网络连接
  private val selector = new KSelector(
    maxRequestSize,
    connectionsMaxIdleMs,
    metrics,
    time,
    "socket-server",
    metricTags,
    false,
    ChannelBuilders.create(protocol, Mode.SERVER, LoginType.SERVER, channelConfigs, null, true))

  override def run() {
    // 1. 标识 Processor 初始化流程结束，唤醒阻塞等待此 Processor 初始化完成的线程
    startupComplete()
    while (isRunning) {
      try {
        // setup any new connections that have been queued up
        // 2. 处理 newConnections 队列中新建 SocketChannel。队列中的每个 SocketChannel
        // 都要在 nioSelector 上注册 OP_READ 事件。
        configureNewConnections()
        // register any new responses for writing
        // 3. 获取 RequestChannel 中对应的 responseQueue 队列，并处理其中缓存的 Response
        processNewResponses()
        // 4. 调用 SocketServer.poll() 方法读取请求，发送响应
        poll()
        // 5. 处理 KSelector.completedReceives 队列
        processCompletedReceives()
        // 6. 处理 KSelector.completedSends 队列
        processCompletedSends()
        // 7. 处理 KSelector.disconnected 队列
        processDisconnected()
      } catch {
        // We catch all the throwables here to prevent the processor thread from exiting. We do this because
        // letting a processor exit might cause a bigger impact on the broker. Usually the exceptions thrown would
        // be either associated with a specific socket channel or a bad request. We just ignore the bad socket channel
        // or request. This behavior might need to be reviewed if we see an exception that need the entire broker to stop.
        case e: ControlThrowable => throw e
        case e: Throwable =>
          error("Processor got uncaught exception.", e)
      }
    }

    debug("Closing selector - processor " + id)
    swallowError(closeAll())
    // 8. 关闭整个 SocketServer，将 alive 设置为 false. 执行一系列关闭操作
    shutdownComplete()
  }

  private def processNewResponses() {
    // 获取对应 responseQueue
    var curr = requestChannel.receiveResponse(id)
    while (curr != null) {
      try {
        curr.responseAction match {
          case RequestChannel.NoOpAction =>
            // 表示此连接暂无响应需要发送，则为 KafkaChannel 注册 OP_READ，允许其继续读取请求
            // There is no response to send to the client, we need to read more pipelined requests
            // that are sitting in the server's socket buffer
            curr.request.updateRequestMetrics
            trace("Socket server received empty response to send, registering for read: " + curr)
            selector.unmute(curr.request.connectionId)
          case RequestChannel.SendAction =>
            // 表示该 Response 需要发送给客户端，需要为其注册 ON_WRITE 事件，并将 `KafkaChannel.send`
            // 字段指向待发送的 Response 对象。同事还会将 Response 从 responseQueue 队列中移出，放入 inflightResponse
            // 中。KafkaChannel.send() 方法中，在发送完一个完整响应后，会取消此连接注册的 OP_WRITE 事件

            // 调用 KSelector.send() 方法，并将响应放入 inflightResponses 队列缓存
            sendResponse(curr)
          case RequestChannel.CloseConnectionAction =>
            curr.request.updateRequestMetrics
            trace("Closing socket connection actively according to the response code.")
            close(selector, curr.request.connectionId)
        }
      } finally {
        // 继续处理 responseQueue
        curr = requestChannel.receiveResponse(id)
      }
    }
  }

  /* `protected` for test usage */
  protected[network] def sendResponse(response: RequestChannel.Response) {
    trace(s"Socket server received response to send, registering for write and sending data: $response")
    val channel = selector.channel(response.responseSend.destination)
    // `channel` can be null if the selector closed the connection because it was idle for too long
    if (channel == null) {
      warn(s"Attempting to send response via channel for which there is no open connection, connection id $id")
      response.request.updateRequestMetrics()
    }
    else {
      selector.send(response.responseSend)
      inflightResponses += (response.request.connectionId -> response)
    }
  }

  private def poll() {
    // 调用的是 KSelelctor.poll()
    try selector.poll(300)
    catch {
      case e @ (_: IllegalStateException | _: IOException) =>
        error(s"Closing processor $id due to illegal state or IO exception")
        swallow(closeAll())
        shutdownComplete()
        throw e
    }
  }

  private def processCompletedReceives() {
    // 遍历 KSelector.completedReceives 队列
    selector.completedReceives.asScala.foreach { receive =>
      try {
        // 获取请求对应的 KafkaChannel
        val channel = selector.channel(receive.source)
        // 创建 KafkaChannel 对应的 Session 对象，与权限控制相关
        val session = RequestChannel.Session(new KafkaPrincipal(KafkaPrincipal.USER_TYPE, channel.principal.getName),
          channel.socketAddress)
        // 将 NetworkReceive。ProcessorId、身份认证信息一起封装成 RequestChannel.Request 对象
        val req = RequestChannel.Request(processor = id, connectionId = receive.source, session = session, buffer = receive.payload, startTimeMs = time.milliseconds, securityProtocol = protocol)
        // req 放入 RequestChannel.requestQueue 队列中，等待 Handler 线程的后续处理
        requestChannel.sendRequest(req)
        // 取消注册的 OP_READ 事件，连接不再读取数据
        selector.mute(receive.source)
      } catch {
        case e @ (_: InvalidRequestException | _: SchemaException) =>
          // note that even though we got an exception, we can assume that receive.source is valid. Issues with constructing a valid receive object were handled earlier
          error(s"Closing socket for ${receive.source} because of error", e)
          close(selector, receive.source)
      }
    }
  }

  private def processCompletedSends() {
    // 遍历 completedSends 队列
    selector.completedSends.asScala.foreach { send =>
      // 此响应已经发送出去（队列中的均为发送出去的），从 inflightResponses 中删除
      val resp = inflightResponses.remove(send.destination).getOrElse {
        throw new IllegalStateException(s"Send for ${send.destination} completed, but not in `inflightResponses`")
      }
      resp.request.updateRequestMetrics()
      // 注册 OP_READ 事件，允许此连接继续读取数据
      selector.unmute(send.destination)
    }
  }

  private def processDisconnected() {
    // 遍历 disconnected 队列
    selector.disconnected.asScala.foreach { connectionId =>
      val remoteHost = ConnectionId.fromString(connectionId).getOrElse {
        throw new IllegalStateException(s"connectionId has unexpected format: $connectionId")
      }.remoteHost
      // 从 INflightResponses 中删除该连接对应的所有 Response
      inflightResponses.remove(connectionId).foreach(_.request.updateRequestMetrics())
      // the channel has been closed by the selector but the quotas still need to be updated
      // 减少 ConnectionQuotas 中对应记录的连接数
      connectionQuotas.dec(InetAddress.getByName(remoteHost))
    }
  }

  /**
   * Queue up a new connection for reading
   */
  def accept(socketChannel: SocketChannel) {
    // 这里调用是由 Acceptor 线程调用添加的
    newConnections.add(socketChannel)
    // 唤醒 Processor 线程，最终调用底层 Java NIO Selector 的 wakeup()
    wakeup()
  }

  /**
   * Register any new connections that have been queued up
   */
  private def configureNewConnections() {
    while (!newConnections.isEmpty) { // 遍历 newConnections 队列
      val channel = newConnections.poll()
      try {
        debug(s"Processor $id listening to new connection from ${channel.socket.getRemoteSocketAddress}")
        val localHost = channel.socket().getLocalAddress.getHostAddress
        val localPort = channel.socket().getLocalPort
        val remoteHost = channel.socket().getInetAddress.getHostAddress
        val remotePort = channel.socket().getPort
        val connectionId = ConnectionId(localHost, localPort, remoteHost, remotePort).toString
        // 注册 OP_READ 事件
        // 在这里，SocketChannel 会被封装为 KafkaChannel，并附加 (attach) 到 SelectionKey 上
        // 所以后面触发 OP_READ 事件时，从 SelectionKey 上获取的是 KafkaChannel 类型的对象
        selector.register(connectionId, channel)
      } catch {
        // We explicitly catch all non fatal exceptions and close the socket to avoid a socket leak. The other
        // throwables will be caught in processor and logged as uncaught exceptions.
        case NonFatal(e) =>
          val remoteAddress = channel.getRemoteAddress
          // need to close the channel here to avoid a socket leak.
          close(channel)
          error(s"Processor $id closed connection from $remoteAddress", e)
      }
    }
  }

  /**
   * Close the selector and all open connections
   */
  private def closeAll() {
    selector.channels.asScala.foreach { channel =>
      close(selector, channel.id)
    }
    selector.close()
  }

  /* For test usage */
  private[network] def channel(connectionId: String): Option[KafkaChannel] =
    Option(selector.channel(connectionId))

  /**
   * Wakeup the thread for selection.
   */
  @Override
  def wakeup = selector.wakeup()

}

class ConnectionQuotas(val defaultMax: Int, overrideQuotas: Map[String, Int]) {

  private val overrides = overrideQuotas.map { case (host, count) => (InetAddress.getByName(host), count) }
  private val counts = mutable.Map[InetAddress, Int]()

  def inc(address: InetAddress) {
    counts.synchronized {
      val count = counts.getOrElseUpdate(address, 0)
      counts.put(address, count + 1)
      val max = overrides.getOrElse(address, defaultMax)
      if (count >= max)
        throw new TooManyConnectionsException(address, max)
    }
  }

  def dec(address: InetAddress) {
    counts.synchronized {
      val count = counts.getOrElse(address,
        throw new IllegalArgumentException(s"Attempted to decrease connection count for address with no connections, address: $address"))
      if (count == 1)
        counts.remove(address)
      else
        counts.put(address, count - 1)
    }
  }

  def get(address: InetAddress): Int = counts.synchronized {
    counts.getOrElse(address, 0)
  }

}

class TooManyConnectionsException(val ip: InetAddress, val count: Int) extends KafkaException("Too many connections from %s (maximum = %d)".format(ip, count))
