package io.github.solayw.spring.etcd

import io.etcd.jetcd.ByteSequence
import io.etcd.jetcd.Client
import io.etcd.jetcd.lease.LeaseKeepAliveResponse
import io.etcd.jetcd.options.PutOption
import io.etcd.jetcd.support.CloseableClient
import io.grpc.stub.StreamObserver
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.InitializingBean
import org.springframework.boot.autoconfigure.AutoConfigureAfter
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.cloud.client.serviceregistry.ServiceRegistry
import org.springframework.context.ApplicationContext
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.core.env.get
import java.net.InetAddress
import java.util.*

object KeepAliveHandler: StreamObserver<LeaseKeepAliveResponse> {
    override fun onCompleted() {
    }

    override fun onError(t: Throwable?) {
    }

    override fun onNext(value: LeaseKeepAliveResponse?) {
    }
}
open class EtcdServiceRegistry(private val etcd: Client, private val prop: EtcdDiscoveryProperties)
    : ServiceRegistry<EtcdServiceInstance>, InitializingBean{

    private var lease = 0L
    private var keepAlive: CloseableClient? = null
    override fun afterPropertiesSet() {
        lease = etcd.leaseClient.grant(30000).get().id
        keepAlive = etcd.leaseClient.keepAlive(lease, KeepAliveHandler)
    }
    private fun EtcdServiceInstance.key(): ByteSequence {
        return "${prop.prefix}.${this.instanceId}".byteSequence()
    }
    override fun register(registration: EtcdServiceInstance) {
        val put = PutOption.newBuilder().withLeaseId(lease).build()
        etcd.kvClient.put(registration.key(), registration.byteSequence(),  put)
            .get()

    }

    override fun deregister(registration: EtcdServiceInstance) {
        etcd.kvClient.delete(registration.key()).get()
    }

    override fun setStatus(registration: EtcdServiceInstance, status: String) {
        registration.status
    }

    override fun close() {
        keepAlive?.close()
        keepAlive = null
    }

    override fun <T : Any> getStatus(registration: EtcdServiceInstance): T? {
        return registration.status as T?
    }
}


@Configuration
@EnableConfigurationProperties(EtcdDiscoveryProperties::class)
@ConditionalOnBean(Client::class)
@AutoConfigureAfter(EtcdAutoConfiguration::class)
@ConditionalOnProperty(prefix = "spring.cloud.discovery.etcd.register", value = ["true"], matchIfMissing = true)
open class EtcdRegisterAutoConfiguration {
    private val logger = LoggerFactory.getLogger(EtcdRegisterAutoConfiguration::class.java)
    @Bean
    open fun etcdRegistration(prop: EtcdDiscoveryProperties, ctx: ApplicationContext): EtcdServiceInstance {
        val uuid = UUID.randomUUID().toString()
        val port = prop.port?: ctx.environment["server.port"]?.toInt()
            ?: throw RuntimeException("unknown service port, define spring.cloud.register.etcd.port or server.port")
        var address = prop.host ?: ctx.environment["server.address"]
        if(address == null) {
            address = InetAddress.getLoopbackAddress().hostAddress
        }
        if(address == null) {
            throw RuntimeException("unknown service address, define spring.cloud.register.etcd.host or server.port")
        }
        val serviceName = ctx.environment["spring.application.name"]
            ?: throw RuntimeException("spring.application.name not defined")

        val ins = EtcdServiceInstance(uuid, serviceName, address, port, false)
        logger.info("new register ${ins.byteSequence()}")
        return ins
    }

    @Bean
    open fun etcdServiceRegistry(etcd: Client, prop: EtcdDiscoveryProperties) = EtcdServiceRegistry(etcd, prop)




}