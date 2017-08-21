//
//  CRCTests.swift
//  FranzTests
//
//  Created by Luke Lau on 02/07/2017.
//

import XCTest
@testable import Franz

class CRCTests: XCTestCase {
    
	func testCRC() {
		let data = "783y2hjbiu89ewrh3bkjnio90u8hbJNIP".data(using: .utf8)!
		let crc = CRC32(data: data).crc
		XCTAssertEqual(0x88bed727, crc)
	}
	
}
