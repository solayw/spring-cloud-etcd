package io.github.solayw.spring.etcd

import io.etcd.jetcd.Client
import org.springframework.boot.SpringApplication
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.boot.context.properties.ConfigurationPropertiesScan
import org.springframework.boot.context.properties.ConstructorBinding
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.core.env.get


@SpringBootApplication
open class App {
    companion object {
        @JvmStatic
        fun main(args: Array<String>) {
            val app = SpringApplication.run(App::class.java)
            app.getBean(EtcdProperties::class.java)
        }
    }
}