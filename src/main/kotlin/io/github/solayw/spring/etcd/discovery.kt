package io.github.solayw.spring.etcd

import io.etcd.jetcd.Client
import io.etcd.jetcd.KeyValue
import io.etcd.jetcd.Watch
import io.etcd.jetcd.options.GetOption
import io.etcd.jetcd.options.WatchOption
import io.etcd.jetcd.watch.WatchEvent
import kotlinx.serialization.decodeFromString
import kotlinx.serialization.json.Json
import org.springframework.beans.factory.InitializingBean
import org.springframework.boot.autoconfigure.AutoConfigureAfter
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.cloud.client.ServiceInstance
import org.springframework.cloud.client.discovery.DiscoveryClient
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
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


@Configuration
@EnableConfigurationProperties(EtcdDiscoveryProperties::class)
@ConditionalOnBean(Client::class)
@AutoConfigureAfter(EtcdAutoConfiguration::class)
@ConditionalOnProperty(prefix = "spring.cloud.discovery.etcd.enabled", value = ["true"], matchIfMissing = true)
open class EtcdDiscoveryAutoConfiguration {
    @Bean
    open fun etcdDiscoveryClient(etcd: Client, prop: EtcdDiscoveryProperties) = EtcdDiscoveryClient(etcd, prop)
}