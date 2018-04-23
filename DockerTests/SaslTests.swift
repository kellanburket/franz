//
//  SaslTests.swift
//  FranzTests
//
//  Created by Luke Lau on 20/04/2018.
//

import XCTest
@testable import Franz

class SaslTests: DockerTestBase {
	
	override class var topics: [String] { return ["test"] }
	override class var yml: URL {
		return Bundle(for: SaslTests.self).url(forResource: "docker-compose-sasl", withExtension: "yml")!
	}
	override class var auth: Cluster.Authentication {
		return .plain(username: "kafka", password: "kafka-secret")
	}
	
	var cluster: Cluster!
	
	func testPlainAuthentication() {
		let config = Connection.Config(ipv4: SaslTests.host,
									   port: SaslTests.port,
									   clientId: "saslClient",
									   authentication: .plain(username: "kafka", password: "kafka-secret"))
		
		do {
			let connection = try Connection(config: config)
			
			let request = FetchRequest(topic: "test")
			let response = connection.writeBlocking(request)!
			XCTAssertEqual(response.topics.first?.topicName, "test")
		} catch {
			XCTFail("Failed to authenticate with PLAIN")
		}
	}
	
	func testInvalidPlainAuthentication() {
		let config = Connection.Config(ipv4: SaslTests.host,
									   port: SaslTests.port,
									   clientId: "saslClient",
									   authentication: .plain(username: "kafka", password: "wrong-secret"))
		do {
			let connection = try Connection(config: config)
			
			let request = TopicMetadataRequest(topic: "test")
			_ = connection.writeBlocking(request)
			XCTFail("Somehow we authenticated with the wrong credentials?")
		} catch Connection.AuthenticationError.authenticationFailed {
		} catch {
			XCTFail("Got some other error: \(error)")
		}
	}
	
	func testWrongMechanism() {
		let config = Connection.Config(ipv4: SaslTests.host,
									   port: SaslTests.port,
									   clientId: "saslClient",
									   authentication: .none)
		do {
			let connection = try Connection(config: config)
			
			let request = TopicMetadataRequest(topic: "test")
			_ = connection.writeBlocking(request)
			XCTFail("Somehow we authenticated with the wrong credentials?")
		} catch Connection.AuthenticationError.authenticationFailed {
		} catch {
			XCTFail("Got some other error: \(error)")
		}
	}

}
