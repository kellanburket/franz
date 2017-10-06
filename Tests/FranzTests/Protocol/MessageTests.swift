//
//  MessageTests.swift
//  FranzTests
//
//  Created by Luke Lau on 21/08/2017.
//

import XCTest
@testable import Franz

class MessageTests: XCTestCase {
    
	func testData() {
		let message = KafkaMessage(value: "test", key: "foo")
		var data = message.data
		let decodedMessage = KafkaMessage(data: &data)
		XCTAssertEqual("test", String(data: decodedMessage.value, encoding: .utf8)!)
		XCTAssertEqual("foo", String(data: decodedMessage.key!, encoding: .utf8)!)
	}
	
	func testMessageSet() {
		let messageSet = MessageSet(values: [MessageSetItem(value: "foo", key: "bar", offset: 42)])
		var data = messageSet.data
		
		let decodedMessageSet = MessageSet(data: &data)
		XCTAssertEqual(1, decodedMessageSet.values.count)
		XCTAssertEqual(42, decodedMessageSet.values[0].offset)
		XCTAssertEqual("foo".data(using: .utf8), decodedMessageSet.values[0].message.value)
		XCTAssertEqual("bar".data(using: .utf8), decodedMessageSet.values[0].message.key!)
	}
    
}
