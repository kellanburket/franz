//
//  SaslTests.swift
//  FranzTests
//
//  Created by Luke Lau on 20/04/2018.
//

import XCTest
@testable import Franz

class SaslTests: DockerTestBase {
	
	override class var port: Int32 { return 9093 }
	
	var cluster: Cluster!
	
	func testPlainAuthentication() {
		let config = Connection.Config(ipv4: SaslTests.host,
									   port: SaslTests.port,
									   clientId: "saslClient",
									   authentication: .plain(username: "kafka", password: "kafka-secret"))
		do {
			let connection = try Connection(config: config)
			
			let request = TopicMetadataRequest(topic: "test")
			let response = connection.writeBlocking(request)
			XCTAssert(response.topics.keys.contains("test"))
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
		} catch {
		}
	}

}
