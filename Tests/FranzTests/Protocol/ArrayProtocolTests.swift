//
//  ArrayProtocolTests.swift
//  FranzTests
//
//  Created by Luke Lau on 16/08/2017.
//

import XCTest
@testable import Franz

class ArrayProtocolTests: XCTestCase {
    
	func testArray() {
		let values: [Int64] = [64, 32, 128]
		
		let array = KafkaArray<Int64>(values)
		
		XCTAssertEqual(8 * 3 + 4, array.dataLength)
		
		var data = array.data
		XCTAssertEqual(array.dataLength, data.count)
		
		let result = KafkaArray<Int64>(data: &data)
		XCTAssertEqual(values, Array(result))
	}
	
	func testEmptyArray() {
		let values = [Int64]()
		
		let array = KafkaArray<Int64>()
		
		XCTAssertEqual(4, array.dataLength)
		
		XCTAssertEqual(Data(bytes: [0, 0, 0, 0]), array.data)
		
		var data = array.data
		
		let result = KafkaArray<Int64>(data: &data)
		XCTAssertEqual(values, Array(result))
	}
    
}
