package io.github.solayw.spring.etcd

import io.etcd.jetcd.ByteSequence
import io.etcd.jetcd.Client
import io.etcd.jetcd.KeyValue
import io.etcd.jetcd.Watch
import io.etcd.jetcd.lease.LeaseKeepAliveResponse
import io.etcd.jetcd.options.GetOption
import io.etcd.jetcd.options.PutOption
import io.etcd.jetcd.options.WatchOption
import io.etcd.jetcd.support.CloseableClient
import io.etcd.jetcd.watch.WatchEvent
import io.grpc.stub.StreamObserver
import kotlinx.serialization.decodeFromString
import kotlinx.serialization.json.Json
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.InitializingBean
import org.springframework.boot.autoconfigure.AutoConfigureAfter
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.cloud.client.ServiceInstance
import org.springframework.cloud.client.discovery.DiscoveryClient
import org.springframework.cloud.client.serviceregistry.ServiceRegistry
import org.springframework.context.ApplicationContext
import org.springframework.context.ApplicationContextAware
import org.springframework.context.Lifecycle
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.core.env.Environment
import org.springframework.core.env.get
import java.net.InetAddress
import java.util.*
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CopyOnWriteArrayList
import javax.annotation.PreDestroy
import kotlin.collections.ArrayList
import kotlin.collections.HashMap


open class EtcdDiscoveryClient(private val etcd: Client,
    private val props: EtcdDiscoveryProperties): DiscoveryClient, InitializingBean {
    private val services: HashMap<String, HashMap<String, ServiceInstance>> = HashMap()
    private var watcher: Watch.Watcher? = null
    @Synchronized
    private  fun onPut(kv: KeyValue) {
        val ins = EtcdServiceInstance.decode(kv.value)
        services.computeIfAbsent(ins.serviceId){HashMap()}[ins.instanceId] = ins
    }
    @Synchronized
    private fun onDelete(kv: KeyValue) {
        val ins = EtcdServiceInstance.decode(kv.value)
        services[ins.serviceId]?.remove(ins.instanceId)
    }
    override fun afterPropertiesSet() {
        val option = GetOption.newBuilder().isPrefix(true).build()
        val kvs = etcd.kvClient.get(props.prefix.byteSequence(), option).get().kvs
        for (kv in kvs) {
            onPut(kv)
        }
        val watchOption = WatchOption.newBuilder().isPrefix(true).build()
        watcher = etcd.watchClient.watch(props.prefix.byteSequence(), watchOption) {
            for (event in it.events) {
                when(event.eventType) {
                    WatchEvent.EventType.PUT -> onPut(event.keyValue)
                    WatchEvent.EventType.DELETE -> onDelete(event.keyValue)
                }
            }
       }
    }


    override fun getInstances(serviceId: String): List<ServiceInstance> {
        val map = services[serviceId]
        return map?.values?.toList() ?: Collections.emptyList()

    }

    override fun getServices(): MutableList<String> {
        return services.keys.toMutableList()
    }
    override fun description(): String {
        return "Etcd Discovery Client"
    }

    @PreDestroy
    fun destroy() {
        watcher?.close()
    }
}

object KeepAliveHandler: StreamObserver<LeaseKeepAliveResponse> {
    override fun onCompleted() {
    }

    override fun onError(t: Throwable?) {
    }

    override fun onNext(value: LeaseKeepAliveResponse?) {
    }
}
open class EtcdServiceRegistry(private val etcd: Client,
                               private val prop: EtcdDiscoveryProperties,
                                private val env: Environment)
    :InitializingBean{

    private var lease = 0L
    private var keepAlive: CloseableClient? = null
    private val registration: EtcdServiceInstance
    init {
        val uuid = UUID.randomUUID().toString()
        val port = prop.port?: env["server.port"]?.toInt()
        ?: throw RuntimeException("unknown service port, define spring.cloud.register.etcd.port or server.port")
        var address = prop.host ?: env["server.address"]
        if(address == null) {
            address = InetAddress.getLoopbackAddress().hostAddress
        }
        if(address == null) {
            throw RuntimeException("unknown service address, define spring.cloud.register.etcd.host or server.port")
        }
        val serviceName = env["spring.application.name"]
            ?: throw RuntimeException("spring.application.name not defined")

        registration = EtcdServiceInstance(uuid, serviceName, address, port, false)
    }

    override fun afterPropertiesSet() {
        lease = etcd.leaseClient.grant(20).get().id
        keepAlive = etcd.leaseClient.keepAlive(lease, KeepAliveHandler)
        val put = PutOption.newBuilder().withLeaseId(lease).build()


        etcd.kvClient.put(registration.key(), registration.byteSequence(),  put)
            .get()
    }

    private fun EtcdServiceInstance.key(): ByteSequence {
        return "${prop.prefix}.${this.instanceId}".byteSequence()
    }

    @PreDestroy
    open fun destroy() {
        etcd.kvClient.delete(registration.key()).get()
        keepAlive?.close()
    }

}


@Configuration
@EnableConfigurationProperties(EtcdDiscoveryProperties::class)
@ConditionalOnBean(Client::class)
@AutoConfigureAfter(EtcdAutoConfiguration::class)
@ConditionalOnProperty(prefix = "spring.cloud.discovery.etcd.enabled", value = ["true"], matchIfMissing = true)
open class EtcdDiscoveryAutoConfiguration {

    @Bean
    open fun etcdDiscoveryClient(etcd: Client, prop: EtcdDiscoveryProperties) = EtcdDiscoveryClient(etcd, prop)


    @Bean
    @ConditionalOnProperty(prefix = "spring.cloud.discovery.etcd.register", value = ["true"], matchIfMissing = true )
    open fun etcdServiceRegistry(etcd: Client, prop: EtcdDiscoveryProperties, ctx:ApplicationContext)
        = EtcdServiceRegistry(etcd, prop, ctx.environment)

}