//
//  VariableLengthProtocolTests.swift
//  FranzTests
//
//  Created by Luke Lau on 15/08/2017.
//

import XCTest
@testable import Franz

class VariableLengthProtocolTests: XCTestCase {
	
	func testDataTake() {
		var data = Data(bytes: [0xff, 0xee, 0xdd, 0xcc])
		
		let taken = data.take(first: 1)
		
		XCTAssertEqual(Data(bytes: [0xff]), taken)
		
		XCTAssertEqual(Data(bytes: [0xee, 0xdd, 0xcc]), data)
		
		let secondTaken = data.take(first: 2)
		
		XCTAssertEqual(Data(bytes: [0xee, 0xdd]), secondTaken)
		
		XCTAssertEqual(Data(bytes: [0xcc]), data)
	}
    
	func testString() {
		let s = "Hello world!"
		var data = s.data
		XCTAssertEqual(s, String(data: &data))
	}
	
	func testEmptyString() {
		let s = ""
		var data = s.data
		XCTAssertEqual(s, String(data: &data))
	}
	
	func testOptionalString() {
		let s: String? = nil
		var data = s.data
		XCTAssertEqual(-1, Int16(data: &data))
		data = s.data
		XCTAssertEqual(nil, String?(data: &data))
		
		let s2: String? = "test"
		var data2 = s2.data
		XCTAssertEqual(4, Int16(data: &data2))
		data2 = s2.data
		XCTAssertEqual("test", String?(data: &data2))
	}
	
	func testOptionalData() {
		let d: Data? = nil
		var data = d.data
		XCTAssertEqual(Data(), Data(data: &data))
		
		let d2: Data? = "test".data(using: .utf8)
		var data2 = d2.data
		XCTAssertEqual(d2, Data(data: &data2))
	}
    
}
