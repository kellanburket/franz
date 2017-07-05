//
//  Extensions.swift
//  Franz
//
//  Created by Kellan Cummings on 1/14/16.
//  Copyright © 2016 Kellan Cummings. All rights reserved.
//

import Foundation


protocol VariableLengthDatable {
    var data: Data { get }
    init()
	static func fromBytes( _ bytes: [UInt8]) -> VariableLengthDatable
}


protocol FixedLengthDatable {
    init(_:Int)
    func toInt() -> Int
    var data: Data { get }
    init(bytes: [UInt8])
}

extension Int8: FixedLengthDatable {
    
	init(bytes: [UInt8]) {
		let data = Data(bytes: bytes, count: 1)
		self.init(bigEndian: data.withUnsafeBytes { $0.pointee })
    }

    func toInt() -> Int {
        return Int(self)
    }
    
    var data: Data {
        var bytes = self
		return Data(bytes: &bytes, count: MemoryLayout<Int8>.size)
    }
}

extension UInt8: FixedLengthDatable {

    init(bytes: [UInt8]) {
		let data = Data(bytes: bytes, count: 1)
		self.init(bigEndian: data.withUnsafeBytes { $0.pointee })
    }
    
    func toInt() -> Int {
        return Int(self)
    }
    
    var data: Data {
        var bytes = self
		return Data(bytes: &bytes, count: MemoryLayout<UInt8>.size)
    }
}

extension Int16: FixedLengthDatable {

	init( bytes: [UInt8]) {
		let data = Data(bytes: bytes, count: 2)
		self.init(bigEndian: data.withUnsafeBytes { $0.pointee })
    }
    
    func toInt() -> Int {
        return Int(self)
    }
    
    var data: Data {
        var bytes = self.bigEndian
		return Data(bytes: &bytes, count: MemoryLayout<Int16>.size)
    }
}

extension UInt16: FixedLengthDatable {
    
    init(bytes: [UInt8]) {
		let data = Data(bytes: bytes, count: 2)
		self.init(bigEndian: data.withUnsafeBytes { $0.pointee })
    }
    
    func toInt() -> Int {
        return Int(self)
    }
    
    var data: Data {
        var bytes = self.bigEndian
		return Data(bytes: &bytes, count: MemoryLayout<UInt16>.size)
    }
}

extension Int32: FixedLengthDatable {

	init(bytes: [UInt8]) {
		let data = Data(bytes: bytes, count: 4)
		self.init(bigEndian: data.withUnsafeBytes { $0.pointee })
    }
    
    func toInt() -> Int {
		return Int(self)
    }
    
    var data: Data {
        var bytes = self.bigEndian
		return Data(bytes: &bytes, count: MemoryLayout<Int32>.size)
    }
}

extension UInt32: FixedLengthDatable {

	init(bytes: [UInt8]) {
		let data = Data(bytes: bytes, count: 4)
		self.init(bigEndian: data.withUnsafeBytes { $0.pointee })
    }
    
    func toInt() -> Int {
        return Int(self)
    }
    
    var data: Data {
        var bytes = self.bigEndian
		return Data(bytes: &bytes, count: MemoryLayout<Int32>.size)
    }
}

extension Int: FixedLengthDatable {

	init(bytes: [UInt8]) {
		let data = Data(bytes: bytes, count: 4)
		self.init(bigEndian: data.withUnsafeBytes { $0.pointee })
    }
    
    func toInt() -> Int {
        return self
    }
   
    init(value: FixedLengthDatable) {
		var dataBytes = value.data
		let data = NSData(bytes: &dataBytes, length: MemoryLayout<FixedLengthDatable>.size)
		
		var out: Int = 0
		data.getBytes(&out, length: MemoryLayout<Int>.size)
		
		self.init(bigEndian: out.bigEndian)
    }
    
    var data: Data {
        var bytes = self.bigEndian
		return Data(bytes: &bytes, count: MemoryLayout<Int>.size)
    }
}

extension Int64: FixedLengthDatable {

	init( bytes: [UInt8]) {
		let data = Data(bytes: bytes, count: 8)
		self.init(bigEndian: data.withUnsafeBytes { $0.pointee })
    }
    
    func toInt() -> Int {
        return Int(self)
    }
    
    var data: Data {
        var bytes = self.bigEndian
		return Data(bytes: &bytes, count: MemoryLayout<Int64>.size)
    }
}

extension UInt64: FixedLengthDatable {

    init(bytes: [UInt8]) {
		let data = Data(bytes: bytes, count: 8)
		self.init(bigEndian: data.withUnsafeBytes { $0.pointee })
    }
    
    func toInt() -> Int {
        return Int(self)
    }
    
    var data: Data {
        var bytes = self.bigEndian
		return Data(bytes: &bytes, count: MemoryLayout<UInt64>.size)
    }
}

extension String: VariableLengthDatable {

    static func fromBytes(_ bytes: [UInt8]) -> VariableLengthDatable {
		var bytes = bytes
		let data = Data(buffer: UnsafeBufferPointer<UInt8>(start: &bytes, count: bytes.count))
        let string = String(data: data, encoding: String.Encoding.utf8) ?? ""
        return self.init(string)
    }
    
    var data: Data {
        return self.data(
            using: String.Encoding.utf8,
            allowLossyConversion: true
        ) ?? Data()
    }
}

extension Data: VariableLengthDatable {

    static func fromBytes(_ bytes: [UInt8]) -> VariableLengthDatable {
		return Data(bytes: bytes)
    }
    
    var data: Data {
        return self
    }
}


extension Stream.Event {
    var description: String {
        switch self {
        case []:
            return "None"
        case .openCompleted:
            return "Open Completed"
        case .hasBytesAvailable:
            return "Has Bytes Available"
        case .hasSpaceAvailable:
            return "Has Space Available"
        case .errorOccurred:
            return "Error Occurred"
        case .endEncountered:
            return "End Encountered"
        default:
            return ""
        }
    }
}

extension Stream.Status {
    var description: String {
        switch self {
        case .notOpen:
            return "Not Open"
        case .opening:
            return "Opening"
        case .open:
            return "Open"
        case .reading:
            return "Reading"
        case .writing:
            return "Writing"
        case .atEnd:
            return  "End"
        case .closed:
            return "Closed"
        case .error:
            return "Error"
        }
    }
}

extension Array {
    
    mutating func slice(_ offset: Int, length: Int) -> [Element] {
        var values = [Element]()
        if self.count >= offset + length {
            for _ in offset..<(offset + length) {
                let value = self.remove(at: offset)
                values.append(value)
            }
        }

        return values
    }
    
}
