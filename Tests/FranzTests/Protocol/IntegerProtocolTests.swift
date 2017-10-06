//
//  KafkaProtocolTests.swift
//  FranzTests
//
//  Created by Luke Lau on 14/08/2017.
//

import XCTest
@testable import Franz

class IntegerProtocolTests: XCTestCase {
	
	//TODO: Rewrite using generic integer type array once SE-0104 is implemented
	//https://github.com/apple/swift-evolution/blob/master/proposals/0104-improved-integers.md
    func testUInt8() {
		for value in [42, UInt8.max, UInt8.min, 0] {
			let int = UInt8(value)
			var data = int.data
			let recreated = UInt8(data: &data)
			XCTAssertEqual(int, recreated)
			
		}
	}
	
	func testUInt16() {
		for value in [42, 0, UInt16.max, UInt16.min] {
			let int = UInt16(value)
			var data = int.data
			let recreated = UInt16(data: &data)
			XCTAssertEqual(int, recreated)
		}
	}
	
	func testUInt32() {
		for value in [42, 0, UInt32.max, UInt32.min] {
			let int = UInt32(value)
			var data = int.data
			let recreated = UInt32(data: &data)
			XCTAssertEqual(int, recreated)
		}
	}
	
	func testUInt64() {
		for value in [42, 0, UInt64.max, UInt64.min] {
			let int = UInt64(value)
			var data = int.data
			let recreated = UInt64(data: &data)
			XCTAssertEqual(int, recreated)
		}
    }
	
	func testUInt() {
		for value in [42, 0, UInt.max, UInt.min] {
			let int = UInt(value)
			var data = int.data
			let recreated = UInt(data: &data)
			XCTAssertEqual(int, recreated)
		}
	}
	
	func testInt8() {
		for value in [42, -42, 0, Int8.max, Int8.min] {
			let int = Int8(value)
			var data = int.data
			let recreated = Int8(data: &data)
			XCTAssertEqual(int, recreated)
		}
	}
	
	func testInt16() {
		for value in [42, -42, 0, Int16.max, Int16.min] {
			let int = Int16(value)
			var data = int.data
			let recreated = Int16(data: &data)
			XCTAssertEqual(int, recreated)
		}
	}
	
	func testInt32() {
		for value in [42, -42, 0, Int32.max, Int32.min] {
			let int = Int32(value)
			var data = int.data
			let recreated = Int32(data: &data)
			XCTAssertEqual(int, recreated)
		}
	}
	
	func testInt64() {
		for value in [42, -42, 0, Int64.max, Int64.min] {
			let int = Int64(value)
			var data = int.data
			let recreated = Int64(data: &data)
			XCTAssertEqual(int, recreated)
		}
	}
	
	func testInt() {
		for value in [42, -42, 0, Int.max, Int.min] {
			let int = Int64(value)
			var data = int.data
			let recreated = Int64(data: &data)
			XCTAssertEqual(int, recreated)
		}
	}
	
}
