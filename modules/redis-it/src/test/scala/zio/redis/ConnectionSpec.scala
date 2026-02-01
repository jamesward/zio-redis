package zio.redis

import com.dimafeng.testcontainers.DockerComposeContainer
import org.testcontainers.DockerClientFactory
import zio._
import zio.redis.RedisError.ProtocolError
import zio.test.Assertion._
import zio.test.TestAspect._
import zio.test._

trait ConnectionSpec extends IntegrationSpec {
  def connectionSuite: Spec[DockerComposeContainer & Redis, RedisError] =
    suite("connection")(
      suite("authenticating")(
        test("auth with 'default' username") {
          for {
            redis <- ZIO.service[Redis]
            res   <- redis.auth("default", "password")
          } yield assert(res)(isUnit)
        },
        test("auth required error") {
          for {
            redisError <- ZIO.serviceWithZIO[Redis](_.ping()).flip.orDieWith(new Throwable(_))
          } yield assertTrue(redisError.asInstanceOf[ProtocolError].message == "Authentication required.")
        }.provideSome[DockerComposeContainer](
          Redis.singleNode,
          singleNodeConfig(IntegrationSpec.SingleNode2),
          ZLayer.succeed(ProtobufCodecSupplier)
        ),
        test("automatic auth") {
          for {
            pong <- ZIO.serviceWithZIO[Redis](_.ping())
          } yield assertTrue(pong == "PONG")
        }.provideSome[DockerComposeContainer](
          Redis.singleNode,
          singleNodeConfig(IntegrationSpec.SingleNode2, Some("asdf")),
          ZLayer.succeed(ProtobufCodecSupplier)
        )
      ),
      suite("clientId")(
        test("get client id") {
          for {
            id <- ZIO.serviceWithZIO[Redis](_.clientId)
          } yield assert(id)(isGreaterThan(0L))
        }
      ),
      test("set and get name") {
        for {
          redis <- ZIO.service[Redis]
          _     <- redis.clientSetName("foo")
          name  <- redis.clientGetName
        } yield assert(name.getOrElse(""))(equalTo("foo"))
      } @@ clusterExecutorUnsupported,
      suite("ping")(
        test("without argument, returns PONG") {
          for {
            pong <- ZIO.serviceWithZIO[Redis](_.ping())
          } yield assertTrue(pong == "PONG")
        },
        test("with an argument, returns the argument") {
          for {
            pong <- ZIO.serviceWithZIO[Redis](_.ping(Some("toto")))
          } yield assertTrue(pong == "toto")
        }
      ),
      suite("reconnect")(
        test("restart redis") {
          for {
            docker     <- ZIO.service[DockerComposeContainer]
            containerId = docker.getContainerByServiceName(IntegrationSpec.SingleNode0).get.getContainerId()
            _           = DockerClientFactory.instance().client().restartContainerCmd(containerId).exec()
            pong       <- ZIO.serviceWithZIO[Redis](_.ping())
          } yield assertTrue(pong == "PONG")
        },
        test("restart password protected redis") {
          for {
            docker     <- ZIO.service[DockerComposeContainer]
            containerId = docker.getContainerByServiceName(IntegrationSpec.SingleNode2).get.getContainerId()
            _           = DockerClientFactory.instance().client().restartContainerCmd(containerId).exec()
            pong       <- ZIO.serviceWithZIO[Redis](_.ping())
          } yield assertTrue(pong == "PONG")
        }.provideSome[DockerComposeContainer](
          Redis.singleNode,
          singleNodeConfig(IntegrationSpec.SingleNode2, Some("asdf")),
          ZLayer.succeed(ProtobufCodecSupplier)
        ),
        test("re-authenticate after idle timeout") {
          // Simulates Heroku-style idle connection timeout:
          // 1. Connection is established with auth
          // 2. Connection sits idle
          // 3. Server closes connection (simulated by killing container)
          // 4. Client tries to send a request
          // 5. Reconnection should happen with re-authentication
          // 6. Subsequent requests should work
          for {
            docker      <- ZIO.service[DockerComposeContainer]
            redis       <- ZIO.service[Redis]
            // Verify initial connection works
            _           <- redis.set("idle-test", "before")
            before      <- redis.get("idle-test").returning[String]
            _           <- assertTrue(before.contains("before"))
            containerId  = docker.getContainerByServiceName(IntegrationSpec.SingleNode2).get.getContainerId()
            dockerClient = DockerClientFactory.instance().client()
            // Simulate idle timeout: kill the server while connection is idle
            _           <- ZIO.attempt(dockerClient.killContainerCmd(containerId).exec()).orDie
            // Wait a moment for the connection to become stale
            _           <- ZIO.sleep(500.millis)
            // Restart the server
            _           <- ZIO.attempt(dockerClient.startContainerCmd(containerId).exec()).orDie
            _           <- ZIO.sleep(2.seconds)
            // Now try to use the connection - this triggers reconnect + re-auth
            result1     <- redis.set("idle-test", "after").either
            result2     <- redis.get("idle-test").returning[String].either
            // Try multiple subsequent requests to verify connection is stable
            results     <- ZIO.foreach(1 to 10) { i =>
                             redis.set(s"key-$i", s"value-$i").either
                           }
            noauthErrors = (result1 :: result2 :: results.toList).collect {
                             case Left(e: ProtocolError) if e.message.contains("NOAUTH")                  => e
                             case Left(e: ProtocolError) if e.message.contains("Authentication required") => e
                           }
          } yield
            // After idle timeout and reconnect, all requests should succeed
            assertTrue(noauthErrors.isEmpty) &&
              assertTrue(result2.exists(_.contains("after")))
        }.provideSome[DockerComposeContainer](
          Redis.singleNode,
          singleNodeConfig(IntegrationSpec.SingleNode2, Some("asdf")),
          ZLayer.succeed(ProtobufCodecSupplier)
        ) @@ TestAspect.withLiveClock @@ TestAspect.timeout(90.seconds),
        test("re-authenticate after connection dies under load") {
          // This test reproduces the re-authentication race condition:
          // 1. Multiple concurrent requests are running continuously
          // 2. Server dies (simulated by kill)
          // 3. During connection.reconnect in onError, new requests get enqueued
          // 4. AUTH is enqueued AFTER these requests
          // 5. Requests sent before AUTH fail with NOAUTH
          //
          // This simulates what happens when you have active traffic and
          // the connection drops (e.g., due to idle timeout, network issues, etc.)
          for {
            docker      <- ZIO.service[DockerComposeContainer]
            redis       <- ZIO.service[Redis]
            _           <- redis.set("load-test", "before")
            containerId  = docker.getContainerByServiceName(IntegrationSpec.SingleNode2).get.getContainerId()
            dockerClient = DockerClientFactory.instance().client()
            errorsRef   <- Ref.make(List.empty[Throwable])
            successRef  <- Ref.make(0)
            // Start multiple parallel request loops FIRST
            loops       <- ZIO.foreach(1 to 5) { i =>
                             (redis
                               .set(s"loop-$i", s"value")
                               .either
                               .flatMap {
                                 case Right(_) => successRef.update(_ + 1)
                                 case Left(e)  => errorsRef.update(e :: _)
                               }
                               .forever)
                               .fork
                           }
            // Let them run for a bit
            _           <- ZIO.sleep(100.millis)
            // Kill the server while requests are flowing
            _           <- ZIO.attempt(dockerClient.killContainerCmd(containerId).exec()).orDie
            _           <- ZIO.sleep(1.second)
            // Restart server
            _           <- ZIO.attempt(dockerClient.startContainerCmd(containerId).exec()).orDie
            // Let reconnection happen while loops are still running
            _           <- ZIO.sleep(3.seconds)
            _           <- ZIO.foreach(loops)(_.interrupt)
            errors      <- errorsRef.get
            successes   <- successRef.get
            noauthErrors = errors.collect {
                             case e: ProtocolError if e.message.contains("NOAUTH")                  => e
                             case e: ProtocolError if e.message.contains("Authentication required") => e
                           }
            finalResult <- redis.ping().either
          } yield {
            println(s"=== Connection Dies Under Load Test ===")
            println(s"Successes: $successes")
            println(s"Total errors: ${errors.size}")
            errors.groupBy(_.getClass.getSimpleName).toList.take(5).foreach { case (k, v) =>
              println(s"  $k: ${v.size}")
            }
            println(s"NOAUTH errors: ${noauthErrors.size}")
            println(s"Final ping: $finalResult")
            // The bug: NOAUTH errors occur because commands race with AUTH
            assertTrue(noauthErrors.isEmpty) &&
            assertTrue(finalResult.isRight)
          }
        }.provideSome[DockerComposeContainer](
          Redis.singleNode,
          singleNodeConfig(IntegrationSpec.SingleNode2, Some("asdf")),
          ZLayer.succeed(ProtobufCodecSupplier)
        ) @@ TestAspect.withLiveClock @@ TestAspect.timeout(90.seconds)
      ) @@ clusterExecutorUnsupported
    ) @@ sequential
}
