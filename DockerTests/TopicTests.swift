//
//  TopicTests.swift
//  DockerTests
//
//  Created by Luke Lau on 23/04/2018.
//

import XCTest
import Franz

class TopicTests: DockerTestBase {

	override class var topics: [String] { return [] }
	
	func testCreateTopics() {
		let cluster = Cluster(brokers: [(TopicTests.host, TopicTests.port)], clientId: "createTopicsTest")
		cluster.createTopic(name: "newTopic", partitions: 1, replicationFactor: 1)
		let topicExpectation = expectation(description: "Lists topics")
		DispatchQueue.main.asyncAfter(deadline: .now() + 2) {
			cluster.listTopics { topics in
				XCTAssert(topics.contains { $0.name == "newTopic" && $0.partitions.count == 1 })
				topicExpectation.fulfill()
			}
		}
		waitForExpectations(timeout: 10)
	}

}
