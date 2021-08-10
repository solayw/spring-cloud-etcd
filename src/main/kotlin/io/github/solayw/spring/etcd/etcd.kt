package io.github.solayw.spring.etcd

import io.etcd.jetcd.Client
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.boot.context.properties.ConfigurationPropertiesScan
import org.springframework.boot.context.properties.ConstructorBinding
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import java.net.URI


@Configuration
@EnableConfigurationProperties(EtcdProperties::class)
@ConditionalOnClass(Client::class)
@ConditionalOnMissingBean(Client::class)
@ConditionalOnProperty(prefix = "spring.etcd.enabled", value = ["true"], matchIfMissing = true)
@ConfigurationPropertiesScan
open class EtcdAutoConfiguration {
    @Bean
    open fun etcdClient(prop: EtcdProperties):Client {
        val uri = prop.uri.split(',').map { URI.create(it) }
        return Client.builder()
            .endpoints(uri)
            .build()
    }
}