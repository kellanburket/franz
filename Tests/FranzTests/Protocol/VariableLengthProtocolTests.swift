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
		XCTAssertEqual("", String(data: &data))
	}
    
}
