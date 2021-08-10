package io.github.solayw.spring.etcd

import io.etcd.jetcd.ByteSequence
import kotlinx.serialization.Serializable
import kotlinx.serialization.decodeFromString
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json

import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.boot.context.properties.ConstructorBinding
import org.springframework.cloud.client.ServiceInstance
import org.springframework.cloud.client.serviceregistry.Registration
import org.springframework.context.annotation.Configuration
import org.springframework.lang.NonNull
import java.net.URI
import java.util.*
import kotlin.text.Charsets.UTF_8

@ConfigurationProperties(prefix = "spring.cloud.discovery.etcd")
open class EtcdDiscoveryProperties
    @ConstructorBinding constructor(
        @NonNull val prefix: String,
        val port: Int? = null,
        val host: String? = null,
)

@ConfigurationProperties(prefix = "spring.etcd")
open class EtcdProperties
    @ConstructorBinding constructor(
    val uri: String
)

val ServiceInstance.uri: URI
    get() = URI.create("${if (this.isSecure) "https" else "http"}://${this.host}:${this.port}")
fun String.byteSequence(): ByteSequence {
    return ByteSequence.from(this, UTF_8)
}
@Serializable
open class EtcdServiceInstance(
    private val instanceId: String,
    private val serviceId: String,
    private val host: String,
    private val port: Int,
    val secure: Boolean,
    ): Registration {
    var status: String? = null
    override fun getUri(): URI = (this as ServiceInstance).uri
    override fun getMetadata(): MutableMap<String, String> = Collections.emptyMap()
    override fun getInstanceId() = instanceId
    override fun getServiceId() = serviceId
    override fun getHost() = host
    override fun isSecure() = secure
    override fun getPort() = port

    fun byteSequence(): ByteSequence {
        return Json.encodeToString(this).byteSequence()
    }


    companion object {
        fun decode(byteSequence: ByteSequence) =
            Json.decodeFromString<EtcdServiceInstance>(byteSequence.toString())
    }
}



