//
//  DockerTestBase.swift
//  FranzTests
//
//  Created by Luke Lau on 21/08/2017.
//

import XCTest
import Foundation
@testable import Franz

class DockerTestBase: XCTestCase {
    
	static var docker: Process!
	static let startedSemaphore = DispatchSemaphore(value: 0)
	
	static let compose = URL(fileURLWithPath: "/usr/local/bin/docker-compose")
	static let jaas = Bundle(for: DockerTestBase.self).url(forResource: "kafka_server_jaas", withExtension: "conf")!
	
	class var yml: URL {
		return Bundle(for: DockerTestBase.self).url(forResource: "docker-compose", withExtension: "yml")!
	}
	
	class var host: String { return "localhost" }
	class var port: Int32 { return 9092 }
	class var topics: [String] { return [] }
	class var auth: Cluster.Authentication { return .none }
	
	override class func setUp() {
		if CommandLine.arguments.contains("--no-docker") {
			return
		}
		do {
			let downDocker = Process()
			downDocker.executableURL = compose
			downDocker.arguments = ["-f", yml.path, "down"]
			downDocker.standardOutput = nil
			try downDocker.run()
			downDocker.waitUntilExit()
			
			docker = Process()
			docker.executableURL = compose
			docker.arguments = ["-f", yml.path, "up"]
			docker.currentDirectoryURL = DockerTestBase.yml.deletingLastPathComponent()
			docker.standardOutput = nil
			
			try docker.run()
			waitForKafka()
			
			let topicRequests = topics.map { CreateTopicsRequest.CreateTopicRequest(topic: $0, numPartitions: 1, replicationFactor: 1) }
			let request = CreateTopicsRequest(requests: topicRequests)
			let connection = try Connection(config: .init(ipv4: host, port: port, clientId: "topicCreationClient", authentication: auth))
			_ = connection.writeBlocking(request)
		} catch {
			fatalError("Couldn't find docker-compose")
		}
	}
	
	class func waitForKafka() {
		var timer: Timer?
		
		var runLoopToStop: CFRunLoop!
		
		DispatchQueue(label: "FranzDockerStreamPoll").async {
			let cancelTimer = Timer.scheduledTimer(withTimeInterval: 60, repeats: false) { _ in
				timer?.invalidate()
				tearDown()
				fatalError("Couldn't connect to Kafka")
			}
			
			timer = Timer.scheduledTimer(withTimeInterval: 1, repeats: true) { _ in
				let config = Connection.Config(ipv4: host, port: port, clientId: "connectionTest", authentication: auth)
				
				do {
					print("Trying to contact Kafka server")
					let connection = try Connection(config: config)
					if let response = connection.writeBlocking(TopicMetadataRequest()),
						response.brokers.count > 0 {
						cancelTimer.invalidate()
						startedSemaphore.signal()
						connection.close()
					}
				} catch {
					
				}
			}
			runLoopToStop = CFRunLoopGetCurrent()
			CFRunLoopRun()
		}
		startedSemaphore.wait()
		
		CFRunLoopStop(runLoopToStop)
		
		print("Kafka server is ready")
		
		timer?.invalidate()
	}
	
	override class func tearDown() {
		do {
			try Process.run(compose, arguments: ["-f", yml.path, "stop"]).waitUntilExit()
		} catch {
			print("Failed to stop containers")
		}
	}
    
}
