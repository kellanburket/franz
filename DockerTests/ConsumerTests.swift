//
//  ConsumerTests.swift
//  FranzTests
//
//  Created by Luke Lau on 01/08/2017.
//

import XCTest
import Franz

class ConsumerTests: DockerTestBase {
	
	var cluster: Cluster!
	
	override func setUp() {
		cluster = Cluster(brokers: [("localhost", 9092)], clientId: "testClient")
	}
	
	func testReceive() {
		let helloExpectation = expectation(description: "Receives 'Hello' message"),
			worldExpectation = expectation(description: "Receives 'World' message"),
			💯Expectation = expectation(description: "Receives '💯' message")
		
		let consumer = cluster.getConsumer(topics: ["test"], groupId: "testGroup")
		consumer.listen { message in
			let string = String(data: message.value, encoding: .utf8)
			if string == "Hello" {
				helloExpectation.fulfill()
			}
			if string == "World" {
				worldExpectation.fulfill()
			}
			if string == "💯" {
				💯Expectation.fulfill()
			}
		}
		
		cluster.sendMessage("test", message: "Hello")
		
		wait(for: [helloExpectation], timeout: 10)
		
		cluster.sendMessage("test", message: "World")
		cluster.sendMessage("test", message: "💯")
		
		wait(for: [worldExpectation, 💯Expectation], timeout: 10)
	}
	
	func testCount() {
		let expectations = (0..<64).map { expectation(description: "Receives '\($0)'") }
		let consumer = cluster.getConsumer(topics: ["test"], groupId: "testGroup")
		consumer.listen { message in
			let i = Int(String(data: message.value, encoding: .utf8)!)!
			print("Consumed \(i)")
			expectations[i].fulfill()
		}
		
		for i in 0..<64 {
			Timer.scheduledTimer(withTimeInterval: TimeInterval(i) / 2, repeats: false) { _ in
				self.cluster.sendMessage("test", message: "\(i)")
				print("Produced \(i)")
			}
		}
		
		waitForExpectations(timeout: 60)
	}
	
	func testFromStart() {
		cluster.sendMessage("fromStart", message: "test")
		
		let consumer1 = cluster.getConsumer(topics: ["fromStart"], groupId: "newgroup")
		
		let e = expectation(description: "Receives message from start")
		
		consumer1.listen(fromStart: true) { message in
			if String(data: message.value, encoding: .utf8)! == "test" {
				e.fulfill()
			}
		}
		
		let consumer2 = cluster.getConsumer(topics: ["fromStart"], groupId: "newGroup")
		
		consumer2.listen(fromStart: false) { _ in
			XCTFail("Shouldn't have received any messages from earlier")
		}
		
		Thread.sleep(forTimeInterval: 10)
		waitForExpectations(timeout: 10)
	}
	
	func testDoesntReceiveUnsubscribedTopics() {

		let consumer = cluster.getConsumer(topics: ["foo"], groupId: "newgroup")

		consumer.listen { _ in
			XCTFail("Shouldn't have received a message")
		}
		
		cluster.sendMessage("test", message: "Foo")

		Thread.sleep(forTimeInterval: 10)
	}
	
}

